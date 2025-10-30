use cloud_p2p_raft::data_structures::Command;

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::Path;

    #[test]
    fn test_send_photo_command() {
        // Simulate a photo as a byte vector
        let photo_data = vec![255, 216, 255, 224]; // Example JPEG header

        // Create a SendPhoto command
        let command = Command::SendPhoto {
            photo_id: "test_photo".to_string(),
            photo_data: photo_data.clone(),
        };

        // Serialize the command
        let serialized = serde_json::to_string(&command).expect("Serialization failed");

        // Deserialize the command
        let deserialized: Command = serde_json::from_str(&serialized).expect("Deserialization failed");

        // Verify the deserialized command matches the original
        if let Command::SendPhoto { photo_id, photo_data } = deserialized {
            assert_eq!(photo_id, "test_photo");
            assert_eq!(photo_data, photo_data);
        } else {
            panic!("Deserialized command is not SendPhoto");
        }
    }

    #[test]
    fn test_photo_saving() {
        // Simulate a photo as a byte vector
        let photo_data = vec![255, 216, 255, 224]; // Example JPEG header
        let photo_path = Path::new("test_photo.jpg");

        // Save the photo to disk
        fs::write(&photo_path, &photo_data).expect("Failed to save photo");

        // Verify the photo was saved correctly
        let saved_data = fs::read(&photo_path).expect("Failed to read saved photo");
        assert_eq!(saved_data, photo_data);

        // Clean up
        fs::remove_file(&photo_path).expect("Failed to delete test photo");
    }

    #[test]
    fn test_send_photo_command_with_file() {
        // Read the photo file
        let photo_path = Path::new("test_image.png");
        let photo_data = fs::read(&photo_path).expect("Failed to read test photo");

        // Create a SendPhoto command
        let command = Command::SendPhoto {
            photo_id: "test_photo".to_string(),
            photo_data: photo_data.clone(),
        };

        // Serialize the command
        let serialized = serde_json::to_string(&command).expect("Serialization failed");

        // Deserialize the command
        let deserialized: Command = serde_json::from_str(&serialized).expect("Deserialization failed");

        // Verify the deserialized command matches the original
        if let Command::SendPhoto { photo_id, photo_data: deserialized_data } = deserialized {
            assert_eq!(photo_id, "test_photo");
            assert_eq!(deserialized_data, photo_data);
        } else {
            panic!("Deserialized command is not SendPhoto");
        }
    }
}