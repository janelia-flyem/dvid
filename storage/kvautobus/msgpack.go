// +build kvautobus
//go:generate msgp

package kvautobus

// --- MessagePack encoding

type Binary []byte

// Ks are Keys in MessagePack encodable format.
type Ks []Binary

// KV is a Key-Value pair in MessagePack encodable format.
type KV [2]Binary

// KVs is a slice of key-value pairs in MessagePack encodable format.
type KVs []KV
