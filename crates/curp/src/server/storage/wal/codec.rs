use std::io;

use clippy_utilities::NumericCast;
use curp_external_api::LogIndex;
use serde::{de::DeserializeOwned, Serialize};
use sha2::{Digest, Sha256};

use super::{
    error::{CorruptType, WALError},
    framed::{Decoder, Encoder},
    util::get_checksum,
};
use crate::log_entry::LogEntry;

/// Invalid frame type
const INVALID: u8 = 0x00;
/// Entry frame type
const ENTRY: u8 = 0x01;
/// Seal frame type
const SEAL: u8 = 0x02;
/// Commit frame type
const COMMIT: u8 = 0x03;
/// The size in bytes of an frame header
const FRAME_HEADER_SIZE: usize = 8;
/// The size in bytes of an sha256 checksum
const CHECK_SUM_SIZE: usize = 32;

/// Getting the frame type
trait FrameType {
    /// Returns the type of this frame
    fn frame_type(&self) -> u8;
}

/// Encoding of frames
trait FrameEncoder {
    /// Encodes the current frame
    fn encode(&self) -> Vec<u8>;
}

/// The WAL codec
#[allow(clippy::upper_case_acronyms)] // The WAL needs to be all upper cases
#[derive(Debug)]
pub(super) struct WAL<C, H = Sha256> {
    /// Frames stored in decoding
    frames: Vec<DataFrameOwned<C>>,
    /// The hasher state for decoding
    hasher: H,
}

/// Union type of WAL frames
#[derive(Debug)]
enum WALFrame<C> {
    /// Data frame type
    Data(DataFrameOwned<C>),
    /// Commit frame type
    Commit(CommitFrame),
}

/// The data frame
///
/// Contains either a log entry or a seal index
#[derive(Debug, Clone)]
#[cfg_attr(test, derive(PartialEq))]
pub(crate) enum DataFrameOwned<C> {
    /// A Frame containing a log entry
    Entry(LogEntry<C>),
    /// A Frame containing the sealed index
    SealIndex(LogIndex),
}

/// The data frame
///
/// Contains either a log entry or a seal index
#[derive(Debug, Clone)]
#[cfg_attr(test, derive(PartialEq))]
pub(crate) enum DataFrame<'a, C> {
    /// A Frame containing a log entry
    Entry(&'a LogEntry<C>),
    /// A Frame containing the sealed index
    SealIndex(LogIndex),
}

/// The commit frame
///
/// This frames contains a SHA256 checksum of all previous frames since last commit
#[derive(Debug)]
struct CommitFrame {
    /// The SHA256 checksum
    checksum: Vec<u8>,
}

impl<C> WAL<C> {
    /// Creates a new WAL codec
    pub(super) fn new() -> Self {
        Self {
            frames: Vec::new(),
            hasher: Sha256::new(),
        }
    }

    /// Gets encoded length and padding length
    ///
    /// This is used to prevent torn write by forcing 8-byte alignment.
    /// The padding length is encoded into byte 6 (bits 48-55) of the
    /// length field, which is the highest byte stored in the 7-byte header.
    /// Bit 7 of that byte indicates padding is present, and bits 0-2
    /// store the number of padding bytes (1-7).
    #[allow(clippy::arithmetic_side_effects)]
    fn encode_frame_size(data_len: usize) -> (usize, usize) {
        let mut encoded_len = data_len;
        let pad_len = (8 - data_len % 8) % 8;
        if pad_len != 0 {
            // encode padding info in byte 6 (bits 48-55): set bit 7 + padding length
            encoded_len |= (0x80 | pad_len) << 48;
        }
        (encoded_len, pad_len)
    }

    /// Decodes the length and padding from a raw header value
    ///
    /// Returns `(data_len, pad_len)` where `data_len` is the actual payload
    /// length and `pad_len` is the number of padding bytes to skip.
    /// The padding info is stored in bits 48-55 (byte 6 of the 7-byte LE
    /// length field in the header).
    #[allow(clippy::arithmetic_side_effects)]
    fn decode_frame_size(raw: u64) -> (usize, usize) {
        let high_byte: usize = ((raw >> 48) & 0xFF).numeric_cast();
        if high_byte & 0x80 != 0 {
            let pad_len = high_byte & 0x07;
            let data_len: usize = (raw & 0x0000_FFFF_FFFF_FFFF).numeric_cast();
            (data_len, pad_len)
        } else {
            (raw.numeric_cast(), 0)
        }
    }
}

impl<C> Encoder<Vec<DataFrame<'_, C>>> for WAL<C>
where
    C: Serialize,
{
    type Error = io::Error;

    /// Encodes a frame
    fn encode(&mut self, frames: Vec<DataFrame<'_, C>>) -> Result<Vec<u8>, Self::Error> {
        let mut frame_data = Vec::new();
        for frame in frames {
            frame_data.extend_from_slice(&frame.encode());
        }
        let commit_frame = CommitFrame::new_from_data(&frame_data);
        frame_data.extend_from_slice(&commit_frame.encode());

        Ok(frame_data)
    }
}

impl<C> Decoder for WAL<C>
where
    C: Serialize + DeserializeOwned,
{
    type Item = Vec<DataFrameOwned<C>>;

    type Error = WALError;

    #[allow(clippy::arithmetic_side_effects)] // the arithmetic only used as slice indices
    fn decode(&mut self, src: &[u8]) -> Result<(Self::Item, usize), Self::Error> {
        let mut cursor = 0;
        while cursor < src.len() {
            let next = src.get(cursor..).ok_or(WALError::MaybeEnded)?;
            let Some((frame, len)) = WALFrame::<C>::decode(next)? else {
                return Err(WALError::MaybeEnded);
            };
            let decoded_bytes = src.get(cursor..cursor + len).ok_or(WALError::MaybeEnded)?;
            cursor += len;
            match frame {
                WALFrame::Data(data) => {
                    self.frames.push(data);
                    self.hasher.update(decoded_bytes);
                }
                WALFrame::Commit(commit) => {
                    let checksum = self.hasher.clone().finalize();
                    self.hasher.reset();
                    if commit.validate(&checksum) {
                        return Ok((self.frames.drain(..).collect(), cursor));
                    }
                    return Err(WALError::Corrupted(CorruptType::Checksum));
                }
            }
        }
        Err(WALError::MaybeEnded)
    }
}

#[allow(
    clippy::indexing_slicing, // Index slicings are checked
    clippy::arithmetic_side_effects, //  Arithmetics are checked
    clippy::unnecessary_wraps // Use the wraps to make code more consistenct
)]
impl<C> WALFrame<C>
where
    C: DeserializeOwned,
{
    /// Decodes a frame from the buffer
    ///
    /// * The frame header memory layout
    ///
    /// 0      1      2      3      4      5      6      7      8
    /// |------+------+------+------+------+------+------+------|
    /// | Type | Length / Index (6 bytes)          | Pad  |
    /// |------+------+------+------+------+------+------+------|
    ///
    /// For Entry frames, byte 7 encodes padding info for 8-byte alignment:
    ///   bit 7: padding flag (1 = padding present)
    ///   bits 0-2: number of padding bytes (1-7)
    /// The actual payload length is stored in bytes 1-6 (48 bits).
    ///
    /// * The frame types
    ///
    /// |------------+-------+-------------------------------------------------------|
    /// | Type       | Value | Desc                                                  |
    /// |------------+-------+-------------------------------------------------------|
    /// | Invalid    |  0x00 | Invalid type                                          |
    /// | Entry      |  0x01 | Stores record of CURP log entry                       |
    /// | Seal Index |  0x02 | Stores the highest index of this current sealed frame |
    /// | Commit     |  0x03 | Stores the checksum                                   |
    /// |------------+-------+-------------------------------------------------------|
    fn decode(src: &[u8]) -> Result<Option<(Self, usize)>, WALError> {
        if src.len() < FRAME_HEADER_SIZE {
            return Ok(None);
        }
        let header: [u8; FRAME_HEADER_SIZE] = src[..FRAME_HEADER_SIZE]
            .try_into()
            .unwrap_or_else(|_| unreachable!("this conversion will always succeed"));
        let frame_type = header[0];
        match frame_type {
            INVALID => Err(WALError::MaybeEnded),
            ENTRY => Self::decode_entry(header, &src[FRAME_HEADER_SIZE..]),
            SEAL => Self::decode_seal_index(header),
            COMMIT => Self::decode_commit(&src[FRAME_HEADER_SIZE..]),
            _ => Err(WALError::Corrupted(CorruptType::Codec(
                "Unexpected frame type".to_owned(),
            ))),
        }
    }

    /// Decodes an entry frame from source
    fn decode_entry(
        header: [u8; FRAME_HEADER_SIZE],
        src: &[u8],
    ) -> Result<Option<(Self, usize)>, WALError> {
        let raw = Self::decode_u64_from_header(header);
        let (data_len, pad_len) = WAL::<C>::decode_frame_size(raw);
        let total_len = data_len + pad_len;
        if src.len() < total_len {
            return Ok(None);
        }
        let payload = &src[..data_len];
        let entry: LogEntry<C> = bincode::deserialize(payload)
            .map_err(|e| WALError::Corrupted(CorruptType::Codec(e.to_string())))?;

        Ok(Some((
            Self::Data(DataFrameOwned::Entry(entry)),
            FRAME_HEADER_SIZE + total_len,
        )))
    }

    /// Decodes an seal index frame from source
    fn decode_seal_index(
        header: [u8; FRAME_HEADER_SIZE],
    ) -> Result<Option<(Self, usize)>, WALError> {
        let index = Self::decode_u64_from_header(header);

        Ok(Some((
            Self::Data(DataFrameOwned::SealIndex(index)),
            FRAME_HEADER_SIZE,
        )))
    }

    /// Decodes a commit frame from source
    fn decode_commit(src: &[u8]) -> Result<Option<(Self, usize)>, WALError> {
        if src.len() < CHECK_SUM_SIZE {
            return Ok(None);
        }
        let checksum = src[..CHECK_SUM_SIZE].to_vec();

        Ok(Some((
            Self::Commit(CommitFrame { checksum }),
            FRAME_HEADER_SIZE + CHECK_SUM_SIZE,
        )))
    }

    /// Gets a u64 from the header
    ///
    /// NOTE: The u64 is encoded using 7 bytes (bits 0-55). For entry frames,
    /// bits 48-55 may contain padding info (see `decode_frame_size`).
    fn decode_u64_from_header(mut header: [u8; FRAME_HEADER_SIZE]) -> u64 {
        header.rotate_left(1);
        header[7] = 0;
        u64::from_le_bytes(header)
    }
}

impl<C> DataFrameOwned<C> {
    /// Converts `DataFrameOwned` to `DataFrame`
    #[cfg(test)]
    pub(super) fn get_ref(&self) -> DataFrame<'_, C> {
        match *self {
            DataFrameOwned::Entry(ref entry) => DataFrame::Entry(entry),
            DataFrameOwned::SealIndex(index) => DataFrame::SealIndex(index),
        }
    }
}

impl<C> FrameType for DataFrame<'_, C> {
    fn frame_type(&self) -> u8 {
        match *self {
            DataFrame::Entry(_) => ENTRY,
            DataFrame::SealIndex(_) => SEAL,
        }
    }
}

impl<C> FrameEncoder for DataFrame<'_, C>
where
    C: Serialize,
{
    #[allow(
        clippy::arithmetic_side_effects,  // The integer shift is safe
        clippy::indexing_slicing // The slicing is checked
    )]
    fn encode(&self) -> Vec<u8> {
        match *self {
            DataFrame::Entry(ref entry) => {
                let entry_bytes = bincode::serialize(entry)
                    .unwrap_or_else(|_| unreachable!("serialization should never fail"));
                let len = entry_bytes.len();
                assert_eq!(len >> 48, 0, "log entry length: {len} too large");
                let (encoded_len, pad_len) = WAL::<C>::encode_frame_size(len);
                let mut bytes =
                    Vec::with_capacity(FRAME_HEADER_SIZE + entry_bytes.len() + pad_len);
                bytes.push(self.frame_type());
                bytes.extend_from_slice(&encoded_len.to_le_bytes()[..7]);
                bytes.extend_from_slice(&entry_bytes);
                bytes.extend_from_slice(&[0; 7][..pad_len]);
                bytes
            }
            DataFrame::SealIndex(index) => {
                assert_eq!(index >> 48, 0, "log index: {index} too large");
                let mut bytes = index.to_le_bytes();
                bytes.rotate_right(1);
                bytes[0] = self.frame_type();
                bytes.to_vec()
            }
        }
    }
}

impl CommitFrame {
    /// Creates a commit frame of data
    fn new_from_data(data: &[u8]) -> Self {
        Self {
            checksum: get_checksum(data).to_vec(),
        }
    }

    /// Validates the checksum
    fn validate(&self, checksum: &[u8]) -> bool {
        *checksum == self.checksum
    }
}

impl FrameType for CommitFrame {
    fn frame_type(&self) -> u8 {
        COMMIT
    }
}

impl FrameEncoder for CommitFrame {
    #[allow(
        clippy::arithmetic_side_effects, // won't overflow
        clippy::indexing_slicing // index position is always valid
    )]
    fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(FRAME_HEADER_SIZE + self.checksum.len());
        bytes.extend_from_slice(&[0; FRAME_HEADER_SIZE]);
        bytes[0] = self.frame_type();
        bytes.extend_from_slice(&self.checksum);
        bytes
    }
}

#[cfg(test)]
mod tests {
    use curp_test_utils::test_cmd::TestCommand;

    use super::*;
    use crate::{log_entry::EntryData, rpc::ProposeId};

    #[tokio::test]
    async fn frame_encode_decode_is_ok() {
        let mut codec = WAL::<TestCommand>::new();
        let entry = LogEntry::<TestCommand>::new(1, 1, ProposeId(1, 2), EntryData::Empty);
        let data_frame = DataFrameOwned::Entry(entry.clone());
        let seal_frame = DataFrameOwned::<TestCommand>::SealIndex(1);
        let mut encoded = codec.encode(vec![data_frame.get_ref()]).unwrap();
        encoded.extend_from_slice(&codec.encode(vec![seal_frame.get_ref()]).unwrap());

        let (data_frame_get, len) = codec.decode(&encoded).unwrap();
        let (seal_frame_get, _) = codec.decode(&encoded[len..]).unwrap();
        let DataFrameOwned::Entry(ref entry_get) = data_frame_get[0] else {
            panic!("frame should be type: DataFrame::Entry");
        };
        let DataFrameOwned::SealIndex(index) = seal_frame_get[0] else {
            panic!("frame should be type: DataFrame::Entry");
        };

        assert_eq!(*entry_get, entry);
        assert_eq!(index, 1);
    }

    #[tokio::test]
    async fn frame_zero_write_will_be_detected() {
        let mut codec = WAL::<TestCommand>::new();
        let entry = LogEntry::<TestCommand>::new(1, 1, ProposeId(1, 2), EntryData::Empty);
        let data_frame = DataFrameOwned::Entry(entry.clone());
        let mut encoded = codec.encode(vec![data_frame.get_ref()]).unwrap();
        encoded[0] = 0;

        let err = codec.decode(&encoded).unwrap_err();
        assert!(matches!(err, WALError::MaybeEnded), "error {err} not match");
    }

    #[tokio::test]
    async fn frame_corrupt_will_be_detected() {
        let mut codec = WAL::<TestCommand>::new();
        let entry = LogEntry::<TestCommand>::new(1, 1, ProposeId(1, 2), EntryData::Empty);
        let data_frame = DataFrameOwned::Entry(entry.clone());
        let mut encoded = codec.encode(vec![data_frame.get_ref()]).unwrap();
        encoded[1] = 0;

        let err = codec.decode(&encoded).unwrap_err();
        assert!(
            matches!(err, WALError::Corrupted(_)),
            "error {err} not match"
        );
    }

    #[test]
    fn encode_frame_size_no_padding_when_aligned() {
        let (encoded_len, pad_len) = WAL::<TestCommand>::encode_frame_size(16);
        assert_eq!(pad_len, 0);
        assert_eq!(encoded_len, 16);
    }

    #[test]
    fn encode_frame_size_adds_padding() {
        let (encoded_len, pad_len) = WAL::<TestCommand>::encode_frame_size(13);
        assert_eq!(pad_len, 3);
        // high byte (bits 48-55) should have 0x83 (0x80 | 3)
        let high_byte = (encoded_len >> 48) & 0xFF;
        assert_eq!(high_byte, 0x83);
        // low 48 bits should be the original length
        assert_eq!(encoded_len & 0x0000_FFFF_FFFF_FFFF, 13);
    }

    #[test]
    fn decode_frame_size_roundtrips() {
        for data_len in [0, 1, 7, 8, 13, 16, 100, 255, 1024, 65535] {
            let (encoded_len, expected_pad) = WAL::<TestCommand>::encode_frame_size(data_len);
            let (decoded_len, decoded_pad) = WAL::<TestCommand>::decode_frame_size(
                encoded_len as u64,
            );
            assert_eq!(decoded_len, data_len, "data_len mismatch for input {data_len}");
            assert_eq!(decoded_pad, expected_pad, "pad_len mismatch for input {data_len}");
        }
    }

    #[tokio::test]
    async fn entry_frame_is_8_byte_aligned() {
        let entry = LogEntry::<TestCommand>::new(1, 1, ProposeId(1, 2), EntryData::Empty);
        let frame = DataFrame::Entry(&entry);
        let encoded = frame.encode();
        assert_eq!(
            encoded.len() % 8, 0,
            "encoded entry frame length {} is not 8-byte aligned",
            encoded.len()
        );
    }

    #[tokio::test]
    async fn padded_entry_frame_encode_decode_roundtrips() {
        let mut codec = WAL::<TestCommand>::new();
        let entry = LogEntry::<TestCommand>::new(1, 1, ProposeId(1, 2), EntryData::Empty);
        let data_frame = DataFrameOwned::Entry(entry.clone());
        let encoded = codec.encode(vec![data_frame.get_ref()]).unwrap();

        // Verify the total encoded batch (data frame + commit frame) is 8-byte aligned
        assert_eq!(
            encoded.len() % 8, 0,
            "total encoded length {} is not 8-byte aligned",
            encoded.len()
        );

        let (frames, _) = codec.decode(&encoded).unwrap();
        assert_eq!(frames.len(), 1);
        let DataFrameOwned::Entry(ref decoded_entry) = frames[0] else {
            panic!("expected Entry frame");
        };
        assert_eq!(*decoded_entry, entry);
    }

    #[tokio::test]
    async fn multiple_padded_frames_decode_correctly() {
        let mut codec = WAL::<TestCommand>::new();
        let entry1 = LogEntry::<TestCommand>::new(1, 1, ProposeId(1, 2), EntryData::Empty);
        let entry2 = LogEntry::<TestCommand>::new(2, 1, ProposeId(3, 4), EntryData::Empty);
        let frame1 = DataFrameOwned::Entry(entry1.clone());
        let frame2 = DataFrameOwned::Entry(entry2.clone());

        // Encode two entry frames in one batch
        let encoded = codec
            .encode(vec![frame1.get_ref(), frame2.get_ref()])
            .unwrap();

        let (frames, _) = codec.decode(&encoded).unwrap();
        assert_eq!(frames.len(), 2);
        let DataFrameOwned::Entry(ref e1) = frames[0] else {
            panic!("expected Entry frame");
        };
        let DataFrameOwned::Entry(ref e2) = frames[1] else {
            panic!("expected Entry frame");
        };
        assert_eq!(*e1, entry1);
        assert_eq!(*e2, entry2);
    }

    #[tokio::test]
    async fn corrupted_padding_detected_by_checksum() {
        let mut codec = WAL::<TestCommand>::new();
        let entry = LogEntry::<TestCommand>::new(1, 1, ProposeId(1, 2), EntryData::Empty);
        let data_frame = DataFrameOwned::Entry(entry.clone());
        let mut encoded = codec.encode(vec![data_frame.get_ref()]).unwrap();

        // Find the padding bytes (between payload end and commit frame start)
        // and corrupt one of them
        let payload_len = bincode::serialize(&entry).unwrap().len();
        let (_, pad_len) = WAL::<TestCommand>::encode_frame_size(payload_len);
        if pad_len > 0 {
            let pad_start = FRAME_HEADER_SIZE + payload_len;
            encoded[pad_start] = 0xFF; // corrupt a padding byte

            let err = codec.decode(&encoded).unwrap_err();
            assert!(
                matches!(err, WALError::Corrupted(CorruptType::Checksum)),
                "expected checksum error, got {err}"
            );
        }
    }
}
