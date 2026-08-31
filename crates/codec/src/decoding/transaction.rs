use crate::DecodingError;
use alloy_primitives::{bytes::Buf, Bytes};
use alloy_rlp::Header;
use dogeos_protocol_types::ScrollTxType;

/// A RLP encoded transaction.
#[derive(Debug)]
pub struct Transaction(pub Bytes);

impl Transaction {
    /// Tries to read an L2 transaction from the input buffer.
    ///
    /// Batch data must not contain L1-message transactions. Those transactions are reconstructed
    /// from the canonical L1 message queue during derivation.
    pub(super) fn try_from_buf(buf: &mut &[u8]) -> Result<Self, DecodingError> {
        // clone the buffer in order to avoid advancing it.
        #[allow(suspicious_double_ref_op)]
        let header = Header::decode(&mut buf.clone()).map_err(|_| DecodingError::Eof)?;
        let finish = |buf: &mut &[u8], offset: usize| {
            if buf.remaining() < offset {
                return Err(DecodingError::Eof)
            }

            // copy the transaction bytes and advance the buffer.
            let tx = Transaction(buf[0..offset].to_vec().into());
            buf.advance(offset);
            Ok(tx)
        };

        if header.list {
            // legacy tx.
            finish(buf, header.length_with_payload())
        } else {
            // typed transaction.
            let mut tx_decode = *buf;
            let tx_type_id = tx_decode.get_u8();
            let tx_type = ScrollTxType::try_from(tx_type_id)
                .map_err(|_| DecodingError::UnsupportedTransactionType(tx_type_id))?;
            if !matches!(
                tx_type,
                ScrollTxType::Eip2930 | ScrollTxType::Eip1559 | ScrollTxType::Eip7702
            ) {
                return Err(DecodingError::UnsupportedTransactionType(tx_type_id))
            }

            let header = Header::decode(&mut tx_decode).map_err(|_| DecodingError::Eof)?;
            if !header.list {
                return Err(DecodingError::InvalidTypedTransactionEncoding)
            }

            finish(buf, header.length_with_payload() + 1)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_only_supported_l2_transaction_types() {
        for transaction in [
            vec![0xc0],
            vec![ScrollTxType::Eip2930 as u8, 0xc0],
            vec![ScrollTxType::Eip1559 as u8, 0xc0],
            vec![ScrollTxType::Eip7702 as u8, 0xc0],
        ] {
            let input = [transaction.as_slice(), &[0xff]].concat();
            let mut buf = input.as_slice();

            let decoded = Transaction::try_from_buf(&mut buf).expect("valid L2 transaction");

            assert_eq!(decoded.0.as_ref(), transaction);
            assert_eq!(buf, &[0xff]);
        }
    }

    #[test]
    fn rejects_l1_message_transaction_without_advancing_buffer() {
        let input = [ScrollTxType::L1Message as u8, 0xc0];
        let mut buf = input.as_slice();

        let result = Transaction::try_from_buf(&mut buf);

        assert!(matches!(
            result,
            Err(DecodingError::UnsupportedTransactionType(tx_type))
                if tx_type == ScrollTxType::L1Message as u8
        ));
        assert_eq!(buf, input);
    }

    #[test]
    fn rejects_unknown_and_malformed_typed_transactions() {
        let unknown = [0x03, 0xc0];
        let mut unknown_buf = unknown.as_slice();
        assert!(matches!(
            Transaction::try_from_buf(&mut unknown_buf),
            Err(DecodingError::UnsupportedTransactionType(0x03))
        ));
        assert_eq!(unknown_buf, unknown);

        let malformed = [ScrollTxType::Eip1559 as u8, 0x80];
        let mut malformed_buf = malformed.as_slice();
        assert!(matches!(
            Transaction::try_from_buf(&mut malformed_buf),
            Err(DecodingError::InvalidTypedTransactionEncoding)
        ));
        assert_eq!(malformed_buf, malformed);
    }
}
