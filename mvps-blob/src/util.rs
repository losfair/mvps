use once_cell::sync::Lazy;
use rand::Rng;
use regex::Regex;
use ulid::Ulid;

static IMAGE_ID_REGEX: Lazy<Regex> =
  Lazy::new(|| Regex::new(r"^[0-9a-zA-Z_-]{1,64}\.mvps-image$").unwrap());

pub fn generate_blob_id() -> String {
  // ULID has 80 bits of randomness, we add another 48 bits so
  // that there are 128 bits of randomness in the id
  format!(
    "{}-{}.mvps-blob",
    Ulid::new().to_string(),
    base32::encode(
      base32::Alphabet::RFC4648 { padding: false },
      &rand::thread_rng().gen::<[u8; 6]>()
    )
  )
}

pub fn generate_image_id() -> String {
  format!(
    "{}-{}.mvps-image",
    Ulid::new().to_string(),
    base32::encode(
      base32::Alphabet::RFC4648 { padding: false },
      &rand::thread_rng().gen::<[u8; 6]>()
    )
  )
}

pub fn is_valid_image_id(image_id: &str) -> bool {
  IMAGE_ID_REGEX.is_match(image_id)
}
