package repository

import (
	"context"
	"fmt"
	"reflect"

	. "github.com/beltran/gohive"
	q "github.com/core-go/hive"
	"github.com/core-go/search"
	"github.com/core-go/search/convert"
	"github.com/core-go/search/template"
	hv "github.com/core-go/search/template/hive"

	. "go-service/internal/model"
)

type UserAdapter struct {
	Connection *Connection
	ModelType   reflect.Type
	FieldsIndex map[string]int
	Schema      *q.Schema
	templates   map[string]*template.Template
}

func NewUserRepository(connection *Connection, templates map[string]*template.Template) (*UserAdapter, error) {
	userType := reflect.TypeOf(User{})
	fieldsIndex, err := q.GetColumnIndexes(userType)
	if err != nil {
		return nil, err
	}
	schema := q.CreateSchema(userType)
	return &UserAdapter{Connection: connection, ModelType: userType, FieldsIndex: fieldsIndex, Schema: schema, templates: templates}, nil
}

func (m *UserAdapter) All(ctx context.Context) ([]User, error) {
	query := "select id, username, email, phone, status, createdDate from users"
	var users []User
	cursor := m.Connection.Cursor()
	err := q.Query(ctx, cursor, m.FieldsIndex, &users, query)
	return users, err
}

func (m *UserAdapter) Load(ctx context.Context, id string) (*User, error) {
	var users []User
	query := fmt.Sprintf("select id, username, email, phone, status , createdDate from users where id = %v ORDER BY id ASC limit 1", id)
	cursor := m.Connection.Cursor()
	err := q.Query(ctx, cursor, m.FieldsIndex, &users, query)
	if err != nil {
		return nil, err
	}
	if len(users) > 0 {
		return &users[0], nil
	}
	return nil, nil
}

func (m *UserAdapter) Create(ctx context.Context, user *User) (int64, error) {
	query := q.BuildToInsert("users", user, m.Schema)
	cursor := m.Connection.Cursor()
	cursor.Exec(ctx, query)
	return 1, cursor.Err
}

func (m *UserAdapter) Update(ctx context.Context, user *User) (int64, error) {
	query := q.BuildToUpdate("users", user, m.Schema)
	cursor := m.Connection.Cursor()
	cursor.Exec(ctx, query)
	return 1, cursor.Err
}

func (m *UserAdapter) Delete(ctx context.Context, id string) (int64, error) {
	cursor := m.Connection.Cursor()
	query := fmt.Sprintf("delete from users where id = %v", id)
	cursor.Exec(ctx, query)
	return 1, cursor.Err
}

func (m *UserAdapter) Search(ctx context.Context, filter *UserFilter) ([]User, int64, error) {
	var rows []User
	if filter.Limit <= 0 {
		return rows, 0, nil
	}
	ftr := convert.ToMap(filter, &m.ModelType)
	query := hv.Build(ftr, *m.templates["user"])
	offset := search.GetOffset(filter.Limit, filter.Page)
	if offset < 0 {
		offset = 0
	}
	pagingQuery := q.BuildPagingQuery(query, filter.Limit, offset)
	countQuery, _ := q.BuildCountQuery(query, nil)

	cursor := m.Connection.Cursor()
	cursor.Exec(ctx, countQuery)
	if cursor.Err != nil {
		return rows, -1, cursor.Err
	}
	var total int64
	for cursor.HasMore(ctx) {
		cursor.FetchOne(ctx, &total)
		if cursor.Err != nil {
			return rows, total, cursor.Err
		}
	}
	err := q.Query(ctx, cursor, m.FieldsIndex, &rows, pagingQuery)
	return rows, total, err
}
