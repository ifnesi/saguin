package store

import (
	"bytes"
	"reflect"
	"testing"
)

// A Will armed with properties sent present and empty keeps them across a
// sessions file, and a file written before the three bits existed - whose
// byte was 0 or 1 - reads the same as it did.
func TestAWillKeepsItsEmptyPropertiesInASessionsFile(t *testing.T) {
	for name, props := range map[string]Props{
		"all three":       {ContentTypeEmpty: true, ResponseTopicEmpty: true, CorrelationDataEmpty: true},
		"one, with a PFI": {CorrelationDataEmpty: true, PayloadFormatFlag: true, PayloadFormat: 1},
		"none":            {PayloadFormatFlag: true, PayloadFormat: 0},
	} {
		t.Run(name, func(t *testing.T) {
			in := &SessionsSnapshot{Sessions: []SessionState{{Session: Session{Client: "a",
				Will: &SessionWill{Topic: "w", Payload: []byte("bye"), Props: props}}}}}
			var buf bytes.Buffer
			if err := in.Encode(&buf); err != nil {
				t.Fatal(err)
			}
			out, err := DecodeSessions(buf.Bytes())
			if err != nil {
				t.Fatal(err)
			}
			if got := out.Sessions[0].Session.Will.Props; !reflect.DeepEqual(got, props) {
				t.Errorf("the Will came back with %+v, want %+v", got, props)
			}
		})
	}
}

// What an encoder wrote before: a byte of 0, or 1 and the indicator. It still
// reads, and the encoder still writes it for a record with neither.
func TestThePropertyByteReadsAsItDid(t *testing.T) {
	for _, tc := range []struct {
		in   Props
		want []byte
	}{
		{Props{}, []byte{0}},
		{Props{PayloadFormatFlag: true, PayloadFormat: 1}, []byte{1, 1}},
		{Props{CorrelationDataEmpty: true}, []byte{8}},
	} {
		e := &encoder{w: new(bytes.Buffer)}
		e.props(tc.in)
		got := e.w.(*bytes.Buffer).Bytes()
		if !bytes.Equal(got, tc.want) {
			t.Errorf("%+v is written as % x, want % x", tc.in, got, tc.want)
		}
		d := &decoder{b: tc.want}
		if p := d.props(); !reflect.DeepEqual(p, tc.in) || d.err != nil {
			t.Errorf("% x reads as %+v (%v), want %+v", tc.want, p, d.err, tc.in)
		}
	}
	d := &decoder{b: []byte{0x10}}
	d.props()
	if d.err == nil {
		t.Error("a byte naming a bit this broker does not know was read")
	}
}
