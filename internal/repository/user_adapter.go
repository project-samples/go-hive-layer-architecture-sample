package repository

import (
	"context"
	"fmt"

	. "github.com/beltran/gohive"

	. "go-service/internal/model"
)

type UserAdapter struct {
	Connection *Connection
}

func NewUserRepository(connection *Connection) *UserAdapter {
	return &UserAdapter{Connection: connection}
}

func (m *UserAdapter) All(ctx context.Context) (*[]User, error) {
	cursor := m.Connection.Cursor()
	query := "select id, username, email, phone, status, createdDate from users"
	cursor.Exec(ctx, query)
	if cursor.Err != nil {
		return nil, cursor.Err
	}
	var result []User
	var user User
	for cursor.HasMore(ctx) {
		cursor.FetchOne(ctx, &user.Id, &user.Username, &user.Email, &user.Phone, &user.Status, &user.CreatedDate)
		if cursor.Err != nil {
			return nil, cursor.Err
		}

		result = append(result, user)
	}
	return &result, nil
}

func (m *UserAdapter) Load(ctx context.Context, id string) (*User, error) {
	cursor := m.Connection.Cursor()
	var user User
	query := fmt.Sprintf("select id, username, email, phone, status , createdDate from users where id = %v ORDER BY id ASC limit 1", id)

	cursor.Exec(ctx, query)
	if cursor.Err != nil {
		return nil, cursor.Err
	}
	for cursor.HasMore(ctx) {
		cursor.FetchOne(ctx, &user.Id, &user.Username, &user.Email, &user.Phone, &user.Status, &user.CreatedDate)
		if cursor.Err != nil {
			return nil, cursor.Err
		}
		return &user, nil
	}
	return nil, nil
}

func (m *UserAdapter) Create(ctx context.Context, user *User) (int64, error) {
	cursor := m.Connection.Cursor()
	query := fmt.Sprintf("INSERT INTO users VALUES (%v, %v, %v, %v, %v, %v)", user.Id, user.Username, user.Email, user.Phone, user.Status, user.CreatedDate)
	cursor.Exec(ctx, query)
	if cursor.Err != nil {
		return -1, cursor.Err
	}
	return 1, nil
}

func (m *UserAdapter) Update(ctx context.Context, user *User) (int64, error) {
	cursor := m.Connection.Cursor()
	query := fmt.Sprintf("UPDATE users SET username = %v, email = %v, phone = %v WHERE id = %v", user.Username, user.Email, user.Phone, user.Id)
	cursor.Exec(ctx, query)
	if cursor.Err != nil {
		return -1, cursor.Err
	}
	return 1, nil
}

func (m *UserAdapter) Delete(ctx context.Context, id string) (int64, error) {
	cursor := m.Connection.Cursor()
	query := fmt.Sprintf("DELETE FROM users WHERE id = %v", id)
	cursor.Exec(ctx, query)
	if cursor.Err != nil {
		return -1, cursor.Err
	}
	return 1, nil
}
