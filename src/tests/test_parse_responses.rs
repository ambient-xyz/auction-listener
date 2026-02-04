use crate::run::{StreamingResponse, SyncResponse};
use serde_json;

#[cfg(test)]
mod tests {
    use crate::run::UsageInfo;

    use super::*;

    #[test]
    fn test_parse_sync_response_basic() {
        let json_input = r#"{
            "id": "chatcmpl-123",
            "object": "chat.completion",
            "created": 1677652288,
            "model": "gpt-4",
            "choices": [
                {
                    "index": 0,
                    "message": {
                        "role": "assistant",
                        "content": "Hello! How can I help you today?"
                    },
                    "finish_reason": "stop"
                }
            ],
            "usage": {
                "prompt_tokens": 10,
                "completion_tokens": 20,
                "total_tokens": 30
            },
            "merkle_root": "0x1234567890abcdef",
            "verified": true
        }"#;

        let parsed: SyncResponse =
            serde_json::from_str(json_input).expect("Failed to parse sync response");

        assert_eq!(parsed.id, "chatcmpl-123");
        assert_eq!(parsed.object, "chat.completion");
        assert_eq!(parsed.created, 1677652288);
        assert_eq!(parsed.model, "gpt-4");
        assert_eq!(parsed.choices.len(), 1);
        assert_eq!(parsed.choices[0].index, 0);
        assert_eq!(parsed.choices[0].message.role, "assistant");
        assert_eq!(
            parsed.choices[0].message.content,
            Some("Hello! How can I help you today?".to_string())
        );
        assert_eq!(
            parsed.usage,
            Some(UsageInfo {
                prompt_tokens: 10,
                completion_tokens: 20,
                total_tokens: 30,
                prompt_tokens_details: None
            })
        );
        assert_eq!(parsed.merkle_root, Some("0x1234567890abcdef".to_string()));
        assert_eq!(parsed.verified, Some(true));
    }

    #[test]
    fn test_parse_sync_response_with_reasoning() {
        let json_input = r#"{
            "id": "chatcmpl-456",
            "object": "chat.completion",
            "created": 1677652300,
            "model": "deepseek-r1",
            "choices": [
                {
                    "index": 0,
                    "message": {
                        "role": "assistant",
                        "reasoning_content": "Let me think about this step by step...",
                        "content": "The answer is 42."
                    },
                    "logprobs": null,
                    "finish_reason": "stop"
                }
            ],
            "usage": {
                "prompt_tokens": 15,
                "completion_tokens": 25,
                "total_tokens": 40
            },
            "merkle_root": "0xabcdef1234567890"
        }"#;

        let parsed: SyncResponse =
            serde_json::from_str(json_input).expect("Failed to parse reasoning response");

        assert_eq!(parsed.id, "chatcmpl-456");
        assert_eq!(parsed.model, "deepseek-r1");
        assert_eq!(
            parsed.choices[0].message.reasoning_content,
            Some("Let me think about this step by step...".to_string())
        );
        assert_eq!(
            parsed.choices[0].message.content,
            Some("The answer is 42.".to_string())
        );
        assert_eq!(parsed.verified, None);
    }

    #[test]
    fn test_parse_sync_response_with_tool_calls() {
        let json_input = r#"{
            "id": "chatcmpl-789",
            "object": "chat.completion",
            "created": 1677652350,
            "model": "gpt-4",
            "choices": [
                {
                    "index": 0,
                    "message": {
                        "role": "assistant",
                        "content": null,
                        "tool_calls": [
                            {
                                "id": "call_abc123",
                                "type": "function",
                                "function": {
                                    "name": "get_weather",
                                    "description": "Get current weather",
                                    "arguments": {
                                        "location": "San Francisco, CA"
                                    }
                                }
                            }
                        ]
                    },
                    "logprobs": null,
                    "finish_reason": "tool_calls"
                }
            ],
            "usage": {
                "prompt_tokens": 25,
                "completion_tokens": 35,
                "total_tokens": 60
            },
            "merkle_root": "0xfedcba0987654321"
        }"#;

        let parsed: SyncResponse =
            serde_json::from_str(json_input).expect("Failed to parse tool calls response");

        assert_eq!(parsed.id, "chatcmpl-789");
        assert_eq!(parsed.choices[0].message.content, None);
        assert_eq!(parsed.choices[0].message.tool_calls.len(), 1);
        assert_eq!(parsed.choices[0].message.tool_calls[0].id, "call_abc123");
        assert_eq!(parsed.choices[0].message.tool_calls[0].type_, "function");
        assert_eq!(
            parsed.choices[0].message.tool_calls[0].function.name,
            "get_weather"
        );
    }

    #[test]
    fn test_parse_streaming_response_content() {
        let json_input = r#"{
            "id": "chatcmpl-stream-123",
            "object": "chat.completion.chunk",
            "created": 1677652400,
            "model": "gpt-4",
            "choices": [
                {
                    "index": 0,
                    "delta": {
                        "content": "Hello"
                    }
                }
            ],
            "usage": {
                "prompt_tokens": 50,
                "completion_tokens": 75,
                "total_tokens": 125,
                "prompt_tokens_details": "cached:10,fresh:40"
            }
        }"#;

        let parsed: StreamingResponse =
            serde_json::from_str(json_input).expect("Failed to parse streaming content");

        match parsed {
            StreamingResponse::Content {
                id,
                object,
                created,
                model,
                choices,
                encryption_iv,
            } => {
                assert_eq!(id, "chatcmpl-stream-123");
                assert_eq!(object, "chat.completion.chunk");
                assert_eq!(created, 1677652400);
                assert_eq!(model, "gpt-4");
                assert_eq!(choices.len(), 1);
                assert_eq!(choices[0].index, 0);
                assert_eq!(encryption_iv, None);

                match &choices[0].delta {
                    crate::run::ContentDelta::Output {
                        role: None,
                        content,
                        tool_calls,
                    } => {
                        assert_eq!(content.as_deref(), Some("Hello"));
                        assert!(tool_calls.is_none());
                    }
                    _ => panic!("Expected output content delta"),
                }
            }
            _ => panic!("Expected streaming content response"),
        }
    }

    #[test]
    fn test_parse_streaming_response_reasoning() {
        let json_input = r#"{
            "id": "chatcmpl-stream-456",
            "object": "chat.completion.chunk",
            "created": 1677652450,
            "model": "deepseek-r1",
            "choices": [
                {
                    "index": 0,
                    "delta": {
                        "reasoning_content": "I need to analyze this problem..."
                    }
                }
            ]
        }"#;

        let parsed: StreamingResponse =
            serde_json::from_str(json_input).expect("Failed to parse streaming reasoning");

        match &parsed {
            StreamingResponse::Content { choices, .. } => match &choices[0].delta {
                crate::run::ContentDelta::Reasoning {
                    role: None,
                    reasoning_content,
                    tool_calls,
                } => {
                    assert_eq!(
                        reasoning_content.as_deref(),
                        Some("I need to analyze this problem...")
                    );
                    assert!(tool_calls.is_none());
                }
                _ => panic!("Expected reasoning content delta"),
            },
            _ => panic!("Expected streaming content response"),
        }

        // now make sure it doesn't output some nulls
        let serialized = serde_json::to_string(&parsed).expect("Failed to serialize back to JSON");
        // should not have tool_calls or role since they were None
        assert!(!serialized.contains("tool_calls"));
        assert!(!serialized.contains("role"));
    }

    #[test]
    fn test_parse_tool_call_streaming() {
        let json_input = r#"{
            "id": "chatcmpl-71ce14c8d1bf4ed8b1b8bf6987926159",
            "object": "chat.completion.chunk",
            "created": 1755029825,
            "model": "deepseek-ai/DeepSeek-R1-0528",
            "choices": [
                {
                    "index": 0,
                    "delta": {
                        "tool_calls": [
                            {
                                "id": "chatcmpl-tool-4d8d4cbf346a45d2a1a2763fb2f51ca4",
                                "type": "function",
                                "index": 0,
                                "function": {
                                    "name": "Bash"
                                }
                            }
                        ]
                    },
                    "logprobs": null,
                    "finish_reason": null
                }
            ]
        }"#;

        let parsed: StreamingResponse =
            serde_json::from_str(json_input).expect("Failed to parse tool call streaming");
        match parsed {
            StreamingResponse::Content { id, choices, .. } => {
                assert_eq!(id, "chatcmpl-71ce14c8d1bf4ed8b1b8bf6987926159");
                assert_eq!(choices.len(), 1);
                match &choices[0].delta {
                    crate::run::ContentDelta::ToolCall {
                        tool_calls,
                        role: None,
                    } => {
                        assert_eq!(tool_calls.len(), 1);
                        assert_eq!(
                            tool_calls[0].id,
                            Some("chatcmpl-tool-4d8d4cbf346a45d2a1a2763fb2f51ca4".to_string())
                        );
                        assert_eq!(tool_calls[0].type_, Some("function".to_string()));
                        assert_eq!(tool_calls[0].function.name, Some("Bash".to_string()));
                        assert_eq!(tool_calls[0].function.arguments, None);
                    }
                    _ => panic!("Expected output content delta"),
                }
            }
            _ => panic!("Expected streaming content response"),
        }
    }

    #[test]
    fn test_parse_tool_call_streaming_part() {
        let json_input = r#"{
            "id": "chatcmpl-71ce14c8d1bf4ed8b1b8bf6987926159",
            "object": "chat.completion.chunk",
            "created": 1755029825,
            "model": "deepseek-ai/DeepSeek-R1-0528",
            "choices": [
                {
                    "index": 0,
                    "delta": {
                        "tool_calls": [
                            {
                                "index": 0,
                                "function": {
                                    "arguments": "{\""
                                }
                            }
                        ]
                    },
                    "logprobs": null,
                    "finish_reason": null
                }
            ]
        }"#;

        let parsed: StreamingResponse =
            serde_json::from_str(json_input).expect("Failed to parse tool call streaming");
        match parsed {
            StreamingResponse::Content { id, choices, .. } => {
                assert_eq!(id, "chatcmpl-71ce14c8d1bf4ed8b1b8bf6987926159");
                assert_eq!(choices.len(), 1);
                match &choices[0].delta {
                    crate::run::ContentDelta::ToolCall {
                        tool_calls,
                        role: None,
                    } => {
                        assert_eq!(tool_calls.len(), 1);
                        assert_eq!(tool_calls[0].type_, None);
                        assert_eq!(tool_calls[0].function.arguments.is_some(), true);
                        assert_eq!(tool_calls[0].function.name, None);
                    }
                    _ => panic!("Expected output content delta"),
                }
            }
            _ => panic!("Expected streaming content response"),
        }
    }

    #[test]
    fn test_parse_streaming_response_with_encryption() {
        let json_input = r#"{
            "id": "chatcmpl-encrypted-789",
            "object": "chat.completion.chunk",
            "created": 1677652500,
            "model": "gpt-4",
            "choices": [
                {
                    "index": 0,
                    "delta": {
                        "content": "SGVsbG8gV29ybGQ="
                    }
                }
            ],
            "encryption_iv": [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16]
        }"#;

        let parsed: StreamingResponse =
            serde_json::from_str(json_input).expect("Failed to parse encrypted streaming");

        match parsed {
            StreamingResponse::Content {
                id,
                choices,
                encryption_iv,
                ..
            } => {
                assert_eq!(id, "chatcmpl-encrypted-789");
                assert_eq!(choices.len(), 1);
                assert!(encryption_iv.is_some());
                assert_eq!(
                    encryption_iv.unwrap(),
                    [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
                );

                match &choices[0].delta {
                    crate::run::ContentDelta::Output {
                        role: None,
                        content,
                        tool_calls,
                    } => {
                        assert_eq!(content.as_deref(), Some("SGVsbG8gV29ybGQ=")); // Base64 encoded content
                        assert!(tool_calls.is_none());
                    }
                    _ => panic!("Expected output content delta"),
                }
            }
            _ => panic!("Expected streaming content response"),
        }
    }

    #[test]
    fn test_parse_streaming_response_tool_calls() {
        let json_input = r#"{
            "id": "chatcmpl-tool-stream-123",
            "object": "chat.completion.chunk",
            "created": 1677652650,
            "model": "gpt-4",
            "choices": [
                {
                    "index": 0,
                    "delta": {
                        "content": "foobar",
                        "tool_calls": [
                            {
                                "id": "call_streaming_abc123",
                                "type": "function",
                                "function": {
                                    "name": "get_weather",
                                    "description": "Get current weather",
                                    "arguments": {
                                        "location": "New York, NY",
                                        "unit": "celsius"
                                    }
                                }
                            }
                        ]
                    }
                }
            ]
        }"#;

        let parsed: StreamingResponse =
            serde_json::from_str(json_input).expect("Failed to parse streaming tool calls");

        match parsed {
            StreamingResponse::Content { id, choices, .. } => {
                assert_eq!(id, "chatcmpl-tool-stream-123");
                assert_eq!(choices.len(), 1);

                match &choices[0].delta {
                    crate::run::ContentDelta::Output {
                        role: None,
                        content,
                        tool_calls,
                    } => {
                        assert_eq!(content.as_deref(), Some("foobar"));
                        let tool_calls = tool_calls.as_ref().expect("Expected tool calls");
                        assert_eq!(tool_calls.len(), 1);
                        assert_eq!(tool_calls[0].id, Some("call_streaming_abc123".to_string()));
                        assert_eq!(tool_calls[0].type_, Some("function".to_string()));
                        assert_eq!(tool_calls[0].function.name.is_some(), true);
                    }
                    _ => panic!("Expected output content delta with tool calls"),
                }
            }
            _ => panic!("Expected streaming content response"),
        }
    }

    #[test]
    fn test_parse_streaming_response_tool_calls_only() {
        let json_input = r#"{
            "id": "chatcmpl-tool-stream-123",
            "object": "chat.completion.chunk",
            "created": 1677652650,
            "model": "gpt-4",
            "choices": [
                {
                    "index": 0,
                    "delta": {
                        "tool_calls": [
                            {
                                "id": "call_streaming_abc123",
                                "type": "function",
                                "function": {
                                    "name": "get_weather",
                                    "description": "Get current weather",
                                    "arguments": {
                                        "location": "New York, NY",
                                        "unit": "celsius"
                                    }
                                }
                            }
                        ]
                    }
                }
            ]
        }"#;

        let parsed: StreamingResponse =
            serde_json::from_str(json_input).expect("Failed to parse streaming tool calls");

        match parsed {
            StreamingResponse::Content { id, choices, .. } => {
                assert_eq!(id, "chatcmpl-tool-stream-123");
                assert_eq!(choices.len(), 1);

                match &choices[0].delta {
                    crate::run::ContentDelta::ToolCall {
                        tool_calls,
                        role: None,
                    } => {
                        assert_eq!(tool_calls.len(), 1);
                        assert_eq!(tool_calls[0].id, Some("call_streaming_abc123".to_string()));
                        assert_eq!(tool_calls[0].type_, Some("function".to_string()));
                        assert_eq!(tool_calls[0].function.name, Some("get_weather".to_string()));
                    }
                    _ => panic!("Expected output content delta with tool calls"),
                }
            }
            _ => panic!("Expected streaming content response"),
        }
    }

    #[test]
    fn test_parse_sync_response_with_logprobs() {
        let json_input = r#"{
            "id": "chatcmpl-logprobs-123",
            "object": "chat.completion",
            "created": 1677652700,
            "model": "gpt-4",
            "choices": [
                {
                    "index": 0,
                    "message": {
                        "role": "assistant",
                        "content": "Hello world!"
                    },
                    "logprobs": {
                        "tokens": ["Hello", " world", "!"],
                        "token_logprobs": [-0.1, -0.2, -0.05],
                        "top_logprobs": [
                            {"Hello": -0.1, "Hi": -0.3},
                            {" world": -0.2, " there": -0.4},
                            {"!": -0.05, ".": -0.15}
                        ]
                    },
                    "finish_reason": "stop"
                }
            ],
            "usage": {
                "prompt_tokens": 5,
                "completion_tokens": 10,
                "total_tokens": 15
            },
            "merkle_root": "0x1122334455667788"
        }"#;

        let parsed: SyncResponse =
            serde_json::from_str(json_input).expect("Failed to parse logprobs response");

        assert_eq!(parsed.id, "chatcmpl-logprobs-123");
        assert_eq!(
            parsed.choices[0].message.content,
            Some("Hello world!".to_string())
        );
        assert!(parsed.choices[0].logprobs.is_some());
    }

    #[test]
    fn test_parse_invalid_response_fails() {
        let invalid_json = r#"{
            "id": "invalid",
            "object": "chat.completion"
            // missing required fields like created, model, choices, etc.
        }"#;

        let result: Result<SyncResponse, _> = serde_json::from_str(invalid_json);
        assert!(result.is_err());
    }
}
