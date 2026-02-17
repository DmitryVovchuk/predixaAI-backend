package connections

import (
	"context"
	"errors"
	"log"
	"os"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	dbconnector "predixaai-backend"
)

type PostgresStore struct {
	pool      *pgxpool.Pool
	encryptor *aesGcmEncryptor
}

func NewResolverFromEnv() (Resolver, error) {
	dsn := os.Getenv("DATABASE_URL")
	key := os.Getenv("ENCRYPTION_KEY")
	if dsn == "" || key == "" {
		return NewResolver(nil), ErrNotConfigured
	}
	enc, err := newAesGcmEncryptor([]byte(key))
	if err != nil {
		return NewResolver(nil), ErrNotConfigured
	}
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		log.Printf("connectionRef lookup disabled")
		return NewResolver(nil), ErrNotConfigured
	}
	store := &PostgresStore{pool: pool, encryptor: enc}
	return NewResolver(store), nil
}

func (s *PostgresStore) GetConnection(ctx context.Context, id string) (dbconnector.ConnectionConfig, error) {
	row := s.pool.QueryRow(ctx, `SELECT type, host, port, user_name, password_enc, database FROM db_connections WHERE id=$1`, id)
	var connType string
	var host string
	var port int
	var user string
	var passwordEnc string
	var database string
	if err := row.Scan(&connType, &host, &port, &user, &passwordEnc, &database); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return dbconnector.ConnectionConfig{}, ErrNotFound
		}
		return dbconnector.ConnectionConfig{}, err
	}
	password, err := s.encryptor.Decrypt(passwordEnc)
	if err != nil {
		return dbconnector.ConnectionConfig{}, errors.New("failed to decrypt password")
	}
	return dbconnector.ConnectionConfig{
		Type:     connType,
		Host:     host,
		Port:     port,
		User:     user,
		Password: password,
		Database: database,
	}, nil
}
