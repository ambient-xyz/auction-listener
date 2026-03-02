use base64::Engine as _;
use base64::prelude::BASE64_STANDARD;
use futures_util::{Stream, StreamExt as _};
use std::net::IpAddr;
use std::time::Duration;
use wolf_crypto::buf::Iv;
use x25519_dalek::{PublicKey, StaticSecret};

use crate::run::{Error, InferenceMessage, MessageContent, StreamingResponse, encrypt_with_iv};

pub const HEADER_PUBLIC_KEY: &str = "X-Node-Public-Key";
pub const HEADER_IV: &str = "X-Encryption-IV";

pub struct NodeEncryption {
    pub secret_key: StaticSecret,
    pub peer_public_key: PublicKey,
}

impl NodeEncryption {
    pub fn shared_secret(&self) -> [u8; 32] {
        self.secret_key
            .diffie_hellman(&self.peer_public_key)
            .to_bytes()
    }

    pub fn own_public_key(&self) -> PublicKey {
        PublicKey::from(&self.secret_key)
    }

    /// Encrypts all message contents in place. Returns metadata for HTTP headers.
    pub fn encrypt_request(
        &self,
        messages: &mut [InferenceMessage],
    ) -> Result<EncryptedRequestMeta, Error> {
        let shared_secret = self.shared_secret();
        let iv: [u8; 16] = rand::random();
        encrypt_messages(messages, shared_secret, iv)?;
        Ok(EncryptedRequestMeta {
            shared_secret,
            iv,
            public_key: self.own_public_key(),
        })
    }
}

pub struct EncryptedRequestMeta {
    pub shared_secret: [u8; 32],
    pub iv: [u8; 16],
    pub public_key: PublicKey,
}

impl EncryptedRequestMeta {
    pub fn apply_headers(&self, builder: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        builder
            .header(
                HEADER_PUBLIC_KEY,
                BASE64_STANDARD.encode(self.public_key.as_bytes()),
            )
            .header(HEADER_IV, BASE64_STANDARD.encode(self.iv))
    }
}

/// Derives shared secret via ECDH, then decrypts all messages in place.
pub fn decrypt_request(
    secret_key: &StaticSecret,
    peer_public_key: PublicKey,
    iv: [u8; 16],
    messages: &mut [InferenceMessage],
) -> Result<[u8; 32], Error> {
    let shared_secret = secret_key.diffie_hellman(&peer_public_key).to_bytes();
    decrypt_messages(messages, shared_secret, iv)?;
    Ok(shared_secret)
}

fn encrypt_messages(
    messages: &mut [InferenceMessage],
    secret: [u8; 32],
    iv: [u8; 16],
) -> Result<(), Error> {
    for m in messages {
        let plaintext = m.content().into_text().join("");
        let encrypted = encrypt_with_iv(plaintext.as_bytes(), secret, Iv::new(iv))?;
        m.set_content(BASE64_STANDARD.encode(encrypted));
    }
    Ok(())
}

fn decrypt_messages(
    messages: &mut [InferenceMessage],
    secret: [u8; 32],
    iv: [u8; 16],
) -> Result<(), Error> {
    for m in messages {
        if let MessageContent::Text(content) = &m.content() {
            let bytes = BASE64_STANDARD
                .decode(content)
                .map_err(|_| Error::InferenceEncryption)?;
            let decrypted = encrypt_with_iv(&bytes, secret, Iv::new(iv))?;
            let text = String::from_utf8(decrypted).map_err(|_| Error::InferenceEncryption)?;
            m.set_content(text);
        }
    }
    Ok(())
}

/// Encrypts a single streaming response event. Pass-through when `shared_secret` is `None`.
pub fn encrypt_response(
    shared_secret: Option<[u8; 32]>,
    event: StreamingResponse,
) -> StreamingResponse {
    let Some(secret) = shared_secret else {
        return event;
    };
    let StreamingResponse::Content {
        id,
        object,
        created,
        model,
        choices,
        ..
    } = event
    else {
        return event;
    };
    let response_iv = rand::random::<[u8; 16]>();
    StreamingResponse::Content {
        id,
        object,
        created,
        model,
        encryption_iv: Some(response_iv),
        choices: choices
            .into_iter()
            .filter_map(|mut c| {
                c.delta
                    .encrypt(secret, Iv::new(response_iv))
                    .inspect_err(|e| tracing::error!("Error encrypting node response: {e}"))
                    .ok()?;
                Some(c)
            })
            .collect(),
    }
}

/// Wraps a stream to decrypt Content chunks using the shared secret.
pub fn decrypt_stream(
    secret: [u8; 32],
    stream: impl Stream<Item = Result<StreamingResponse, Error>>,
) -> impl Stream<Item = Result<StreamingResponse, Error>> {
    stream.map(move |item| {
        let Ok(StreamingResponse::Content {
            id,
            object,
            created,
            model,
            mut choices,
            encryption_iv: Some(iv),
        }) = item
        else {
            return item;
        };
        for choice in &mut choices {
            if let Err(e) = choice.delta.decrypt(secret, Iv::new(iv)) {
                tracing::error!("Failed to decrypt node response: {e}");
                return Err(Error::InferenceEncryption);
            }
        }
        Ok(StreamingResponse::Content {
            id,
            object,
            created,
            model,
            choices,
            encryption_iv: None,
        })
    })
}

/// Queries a node's `/public-key` endpoint. Returns `None` on any failure.
pub async fn discover(
    self_private_key: Option<[u8; 32]>,
    data_ip: IpAddr,
    data_port: u16,
) -> Option<NodeEncryption> {
    let secret_key = StaticSecret::from(self_private_key?);
    let resp = reqwest::Client::new()
        .get(format!("http://{data_ip}:{data_port}/public-key"))
        .timeout(Duration::from_secs(2))
        .send()
        .await
        .ok()?;
    if !resp.status().is_success() {
        return None;
    }
    let b64 = resp.text().await.ok()?;
    let bytes: [u8; 32] = BASE64_STANDARD.decode(b64.trim()).ok()?.try_into().ok()?;
    Some(NodeEncryption {
        secret_key,
        peer_public_key: PublicKey::from(bytes),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::run::ContentChoice;
    use futures_util::stream;

    fn keypair() -> (StaticSecret, PublicKey) {
        let sk = StaticSecret::random();
        let pk = PublicKey::from(&sk);
        (sk, pk)
    }

    fn user_msg(text: &str) -> InferenceMessage {
        InferenceMessage::User {
            content: Some(MessageContent::Text(text.to_owned())),
        }
    }

    fn content_event(text: &str) -> StreamingResponse {
        StreamingResponse::Content {
            id: "id".into(),
            object: "obj".into(),
            created: 0,
            model: "model".into(),
            choices: vec![ContentChoice {
                index: 0,
                delta: crate::run::ContentDelta::Output {
                    role: None,
                    content: Some(text.to_owned()),
                    tool_calls: None,
                },
                logprobs: None,
                finish_reason: None,
                stop_reason: None,
                token_ids: None,
            }],
            encryption_iv: None,
        }
    }

    #[test]
    fn shared_secret_is_symmetric() {
        let (sk_a, pk_a) = keypair();
        let (sk_b, pk_b) = keypair();

        let enc_a = NodeEncryption {
            secret_key: sk_a,
            peer_public_key: pk_b,
        };
        let enc_b = NodeEncryption {
            secret_key: sk_b,
            peer_public_key: pk_a,
        };

        assert_eq!(enc_a.shared_secret(), enc_b.shared_secret());
    }

    #[test]
    fn encrypt_request_then_decrypt() {
        let (sk_a, pk_a) = keypair();
        let (sk_b, pk_b) = keypair();

        let enc = NodeEncryption {
            secret_key: sk_a,
            peer_public_key: pk_b,
        };
        let mut messages = vec![user_msg("hello world")];
        let meta = enc.encrypt_request(&mut messages).unwrap();

        // Messages should now be base64 ciphertext, not plaintext.
        let ciphertext = messages[0].content().into_text().join("");
        assert_ne!(ciphertext, "hello world");

        // Decrypt from the other side.
        decrypt_request(
            &StaticSecret::from(sk_b.to_bytes()),
            pk_a,
            meta.iv,
            &mut messages,
        )
        .unwrap();
        let plaintext = messages[0].content().into_text().join("");
        assert_eq!(plaintext, "hello world");
    }

    #[test]
    fn encrypt_response_roundtrip() {
        let secret: [u8; 32] = rand::random();
        let event = content_event("token");

        let encrypted = encrypt_response(Some(secret), event);
        let StreamingResponse::Content {
            encryption_iv: Some(_),
            ..
        } = &encrypted
        else {
            panic!("expected encrypted content");
        };

        // Feed it through decrypt_stream.
        let stream = stream::once(async { Ok(encrypted) });
        let mut decrypted_stream = std::pin::pin!(decrypt_stream(secret, stream));

        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let result = rt.block_on(async { decrypted_stream.next().await.unwrap().unwrap() });

        let StreamingResponse::Content {
            choices,
            encryption_iv: None,
            ..
        } = result
        else {
            panic!("expected decrypted content");
        };
        let crate::run::ContentDelta::Output {
            content: Some(text),
            ..
        } = &choices[0].delta
        else {
            panic!("expected output delta");
        };
        assert_eq!(text, "token");
    }

    #[test]
    fn encrypt_response_passthrough_when_none() {
        let event = StreamingResponse::Done;
        let result = encrypt_response(Some(rand::random()), event);
        assert!(matches!(result, StreamingResponse::Done));
    }

    #[test]
    fn encrypt_response_passthrough_when_no_secret() {
        let event = content_event("plain");
        let result = encrypt_response(None, event);
        let StreamingResponse::Content {
            encryption_iv: None,
            ..
        } = result
        else {
            panic!("expected unencrypted content");
        };
    }
}
