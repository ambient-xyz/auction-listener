use crate::run::InferenceArgs;
use serde_json;

#[cfg(test)]
mod tests {
    use crate::run::InferenceMessage;

    use super::*;

    #[test]
    fn test_parse_basic_chat_completion_request() {
        let json_input = r#"{
            "model": "gpt-4",
            "messages": [
                {
                    "role": "user",
                    "content": "Hello, how are you?"
                }
            ],
            "temperature": 0.7,
            "max_completion_tokens": 150
        }"#;

        let parsed: InferenceArgs =
            serde_json::from_str(json_input).expect("Failed to parse basic request");

        assert_eq!(parsed.model, Some("gpt-4".to_string()));
        assert_eq!(parsed.messages.len(), 1);
        assert!(matches!(parsed.messages[0], InferenceMessage::User { .. }));
        assert_eq!(parsed.temperature, Some(0.7));
        assert_eq!(parsed.max_completion_tokens, Some(150));
    }

    #[test]
    fn test_parse_streaming_request() {
        let json_input = r#"{
            "model": "gpt-3.5-turbo",
            "messages": [
                {
                    "role": "system",
                    "content": "You are a helpful assistant."
                },
                {
                    "role": "user", 
                    "content": "Write a short poem about the ocean."
                }
            ],
            "stream": true,
            "stream_options": {
                "include_usage": true,
                "continuous_usage_stats": false
            },
            "temperature": 0.9,
            "top_p": 0.95
        }"#;

        let parsed: InferenceArgs =
            serde_json::from_str(json_input).expect("Failed to parse streaming request");

        assert_eq!(parsed.model, Some("gpt-3.5-turbo".to_string()));
        assert_eq!(parsed.stream, Some(true));
        assert_eq!(parsed.messages.len(), 2);
        assert_eq!(parsed.temperature, Some(0.9));
        assert_eq!(parsed.top_p, Some(0.95));
        assert!(parsed.stream_options.is_some());
        assert_eq!(parsed.stream_options.unwrap().include_usage, Some(true));
    }

    #[test]
    fn test_parse_request_with_tools() {
        let json_input = r#"{
            "model": "gpt-4",
            "messages": [
                {
                    "role": "user",
                    "content": "What's the weather like in San Francisco?"
                }
            ],
            "tools": [
                {
                    "type": "function",
                    "function": {
                        "name": "get_current_weather",
                        "description": "Get the current weather in a given location",
                        "parameters": {
                            "type": "object",
                            "properties": {
                                "location": {
                                    "type": "string",
                                    "description": "The city and state, e.g. San Francisco, CA"
                                },
                                "unit": {
                                    "type": "string",
                                    "enum": ["celsius", "fahrenheit"]
                                }
                            },
                            "required": ["location"]
                        }
                    }
                }
            ],
            "tool_choice": "auto"
        }"#;

        let parsed: InferenceArgs =
            serde_json::from_str(json_input).expect("Failed to parse tools request");

        assert_eq!(parsed.model, Some("gpt-4".to_string()));
        assert_eq!(parsed.messages.len(), 1);
        assert!(parsed.tools.is_some());
        assert_eq!(parsed.tools.as_ref().unwrap().len(), 1);
        assert_eq!(
            parsed.tools.as_ref().unwrap()[0].function.name,
            "get_current_weather"
        );
        assert!(parsed.tool_choice.is_some());
    }

    #[test]
    fn test_parse_request_with_structured_content() {
        let json_input = r#"{
            "model": "gpt-4",
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": "Describe what you see in this image:"
                        },
                        {
                            "type": "text", 
                            "text": "Focus on the colors and composition."
                        }
                    ]
                }
            ],
            "temperature": 0.5
        }"#;

        let parsed: InferenceArgs =
            serde_json::from_str(json_input).expect("Failed to parse structured content request");

        assert_eq!(parsed.model, Some("gpt-4".to_string()));
        assert_eq!(parsed.messages.len(), 1);
        assert_eq!(parsed.temperature, Some(0.5));

        // Verify the structured content was parsed correctly
        match &parsed.messages[0].content() {
            crate::run::MessageContent::Segments(segments) => {
                assert_eq!(segments.len(), 2);
                assert_eq!(segments[0].text, "Describe what you see in this image:");
                assert_eq!(segments[1].text, "Focus on the colors and composition.");
            }
            _ => panic!("Expected structured content with segments"),
        }
    }

    #[test]
    fn test_parse_minimal_request() {
        let json_input = r#"{
            "messages": [
                {
                    "role": "user",
                    "content": "Hi"
                }
            ]
        }"#;

        let parsed: InferenceArgs =
            serde_json::from_str(json_input).expect("Failed to parse minimal request");

        assert_eq!(parsed.model, None);
        assert_eq!(parsed.messages.len(), 1);
        assert!(matches!(parsed.messages[0], InferenceMessage::User { .. }));
        assert_eq!(parsed.temperature, None);
        assert_eq!(parsed.stream, None);
    }

    #[test]
    fn test_parse_request_with_custom_fields() {
        let json_input = r#"{
            "model": "custom-model",
            "messages": [
                {
                    "role": "user",
                    "content": "Test message"
                }
            ],
            "wait_for_verification": true,
            "emit_usage": true,
            "emit_verified": false,
            "encrypt_with": [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32]
        }"#;

        let parsed: InferenceArgs =
            serde_json::from_str(json_input).expect("Failed to parse custom fields request");

        assert_eq!(parsed.model, Some("custom-model".to_string()));
        assert_eq!(parsed.wait_for_verification, Some(true));
        assert_eq!(parsed.emit_usage, Some(true));
        assert_eq!(parsed.emit_verified, Some(false));
        assert!(parsed.encrypt_with.is_some());
        assert_eq!(parsed.encrypt_with.unwrap()[0], 1);
        assert_eq!(parsed.encrypt_with.unwrap()[31], 32);
    }

    #[test]
    fn test_parse_invalid_json_fails() {
        let invalid_json = r#"{
            "model": "gpt-4",
            "messages": [
                {
                    "role": "user"
                }
            ]
        }"#;

        let result: Result<InferenceArgs, _> = serde_json::from_str(invalid_json);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_empty_messages_array() {
        let json_input = r#"{
            "model": "gpt-4",
            "messages": [],
            "temperature": 0.7
        }"#;

        let parsed: InferenceArgs =
            serde_json::from_str(json_input).expect("Failed to parse empty messages");

        assert_eq!(parsed.model, Some("gpt-4".to_string()));
        assert_eq!(parsed.messages.len(), 0);
        assert_eq!(parsed.temperature, Some(0.7));
    }
}
