package repository

import (
	"context"
	"fmt"
	"reflect"

	. "github.com/beltran/gohive"
	q "github.com/core-go/hive"
	"github.com/core-go/search/convert"
	"github.com/core-go/search/template"
	hv "github.com/core-go/search/template/hive"

	. "go-service/internal/model"
)

type UserAdapter struct {
	Connection *Connection
	ModelType   reflect.Type
	FieldsIndex map[string]int
	Fields      string
	Schema      *q.Schema
	templates   map[string]*template.Template
}

func NewUserRepository(connection *Connection, templates map[string]*template.Template) (*UserAdapter, error) {
	userType := reflect.TypeOf(User{})
	fieldsIndex, schema, _, _, _, fields, err := q.Init(userType)
	if err != nil {
		return nil, err
	}
	return &UserAdapter{Connection: connection, ModelType: userType, FieldsIndex: fieldsIndex, Fields: fields, Schema: schema, templates: templates}, nil
}

func (m *UserAdapter) All(ctx context.Context) ([]User, error) {
	query := fmt.Sprintf("select %s from users", m.Fields)
	var users []User
	cursor := m.Connection.Cursor()
	err := q.Query(ctx, cursor, m.FieldsIndex, &users, query)
	return users, err
}

func (m *UserAdapter) Load(ctx context.Context, id string) (*User, error) {
	var users []User
	query := fmt.Sprintf("select %s from users where id = %s order by id asc limit 1", m.Fields, id)
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
	var users []User
	if filter.Limit <= 0 {
		return users, 0, nil
	}
	ftr := convert.ToMapWithFields(filter, m.Fields, &m.ModelType)
	query := hv.Build(ftr, *m.templates["user"])
	offset := q.GetOffset(filter.Limit, filter.Page)
	pagingQuery := q.BuildPagingQuery(query, filter.Limit, offset)
	countQuery := q.BuildCountQuery(query)

	cursor := m.Connection.Cursor()
	total, err := q.Count(ctx, cursor, countQuery)
	if err != nil {
		return users, total, err
	}
	err = q.Query(ctx, cursor, m.FieldsIndex, &users, pagingQuery)
	return users, total, err
}
