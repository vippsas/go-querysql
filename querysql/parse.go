package querysql

import (
	"errors"

	"github.com/google/uuid"
)

func ParseSQLUUIDBytes(v []uint8) (uuid.UUID, error) {
	if len(v) != 16 {
		return uuid.UUID{}, errors.New("ParseSQLUUIDBytes: did not get 16 bytes")
	}
	var shuffled [16]uint8
	// This: select convert(uniqueidentifier, '00010203-0405-0607-0809-0a0b0c0d0e0f')
	// Returns this when passed to uuid.FromBytes:
	// 03020100-0504-0706-0809-0a0b0c0d0e0f
	// So, shuffling first
	shuffled[0x0] = v[0x3]
	shuffled[0x1] = v[0x2]
	shuffled[0x2] = v[0x1]
	shuffled[0x3] = v[0x0]

	shuffled[0x4] = v[0x5]
	shuffled[0x5] = v[0x4]

	shuffled[0x6] = v[0x7]
	shuffled[0x7] = v[0x6]

	// The rest are not shuffled :shrug:
	shuffled[0x8] = v[0x8]
	shuffled[0x9] = v[0x9]

	shuffled[0xa] = v[0xa]
	shuffled[0xb] = v[0xb]
	shuffled[0xc] = v[0xc]
	shuffled[0xd] = v[0xd]
	shuffled[0xe] = v[0xe]
	shuffled[0xf] = v[0xf]

	return uuid.FromBytes(shuffled[:])
}
