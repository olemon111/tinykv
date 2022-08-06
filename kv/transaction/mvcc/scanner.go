package mvcc

import (
	"bytes"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	// Your Data Here (4C).
	iter    engine_util.DBIterator
	nextKey []byte
	txn     *MvccTxn
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	reader := txn.Reader
	iter := reader.IterCF(engine_util.CfWrite)
	return &Scanner{
		txn:     txn,
		iter:    iter,
		nextKey: startKey,
	}
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	scan.iter.Close()
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	if !scan.iter.Valid() { // scanner exhausted
		return nil, nil, nil
	}
	scan.iter.Seek(EncodeKey(scan.nextKey, scan.txn.StartTS))
	if !scan.iter.Valid() {
		return nil, nil, nil
	}
	// find key with commit ts <= start ts
	curKey := DecodeUserKey(scan.iter.Item().Key())
	commitTs := scan.txn.StartTS
	for ; commitTs > scan.txn.StartTS; curKey, commitTs = DecodeUserKey(scan.iter.Item().Key()), decodeTimestamp(scan.iter.Item().Key()) {
		scan.iter.Seek(EncodeKey(curKey, scan.txn.StartTS)) // may move to next key
		if !scan.iter.Valid() {
			return nil, nil, nil
		}
	}
	// get write value
	val, err := scan.iter.Item().Value()
	if err != nil || val == nil {
		return curKey, nil, err
	}
	write, err := ParseWrite(val)
	if err != nil || write == nil {
		return curKey, nil, err
	}
	scan.updateNextKey(curKey)
	if write.Kind != WriteKindPut { // delete or rollback
		return curKey, nil, nil
	}
	val, err = scan.txn.GetValue(curKey)
	return curKey, val, err
}

func (scan *Scanner) updateNextKey(curKey []byte) {
	// update nextKey
	for ; scan.iter.Valid(); scan.iter.Next() {
		nextKey := DecodeUserKey(scan.iter.Item().Key())
		if bytes.Compare(nextKey, curKey) != 0 { // move to the first different user key
			scan.nextKey = nextKey
			break
		}
	}
}
