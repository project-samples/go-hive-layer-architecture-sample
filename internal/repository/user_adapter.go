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
	templates   map[string]*template.Template
}

func NewUserRepository(connection *Connection, templates map[string]*template.Template) (*UserAdapter, error) {
	userType := reflect.TypeOf(User{})
	fieldsIndex, err := q.GetColumnIndexes(userType)
	if err != nil {
		return nil, err
	}
	return &UserAdapter{Connection: connection, ModelType: userType, FieldsIndex: fieldsIndex, templates: templates}, nil
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

func (m *UserAdapter) Search(ctx context.Context, filter *UserFilter) ([]User, int64, error) {
	var rows []User
	if filter.Limit <= 0 {
		return rows, 0, nil
	}

	filter.Sort = q.BuildSort(filter.Sort, m.ModelType)
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
