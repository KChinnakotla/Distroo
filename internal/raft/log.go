// basic memory log implementation and operations

package raft


// Log is a 1-based index log with a dummy entry at index 0 for simplicity.
type Log struct {
	ents []Entry // ents[0] is a dummy {Index:0,Term:0}
}

func NewLog() *Log {
	return &Log{ents: []Entry{{Index: 0, Term: 0}}}
}

func (l *Log) LastIndex() uint64 {
	return l.ents[len(l.ents)-1].Index
}

func (l *Log) LastTerm() uint64 {
	return l.ents[len(l.ents)-1].Term
}

func (l *Log) Term(i uint64) uint64 {
	if i >= uint64(len(l.ents)) {
		return 0
	}
	return l.ents[i].Term
}

func (l *Log) Slice(fromIdx uint64) []Entry {
	if fromIdx > l.LastIndex() {
		return nil
	}
	return append([]Entry(nil), l.ents[fromIdx:]...)
}

func (l *Log) Append(es ...Entry) {
	l.ents = append(l.ents, es...)
}

func (l *Log) TruncateFrom(i uint64) {
	if i <= l.LastIndex() {
		l.ents = l.ents[:i]
	}
}

func (l *Log) At(i uint64) (Entry, bool) {
	if i == 0 || i > l.LastIndex() {
		return Entry{}, false
	}
	return l.ents[i], true
}
