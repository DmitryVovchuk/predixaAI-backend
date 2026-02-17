package connections

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"errors"
)

type aesGcmEncryptor struct {
	key []byte
}

func newAesGcmEncryptor(key []byte) (*aesGcmEncryptor, error) {
	if len(key) != 32 {
		return nil, errors.New("encryption key must be 32 bytes")
	}
	return &aesGcmEncryptor{key: key}, nil
}

func (e *aesGcmEncryptor) Decrypt(cipherText string) (string, error) {
	data, err := base64.StdEncoding.DecodeString(cipherText)
	if err != nil {
		return "", err
	}
	block, err := aes.NewCipher(e.key)
	if err != nil {
		return "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	if len(data) < gcm.NonceSize() {
		return "", errors.New("ciphertext too short")
	}
	nonce := data[:gcm.NonceSize()]
	enc := data[gcm.NonceSize():]
	plain, err := gcm.Open(nil, nonce, enc, nil)
	if err != nil {
		return "", err
	}
	return string(plain), nil
}
