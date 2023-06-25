package app

import (
	"context"
	"reflect"

	. "github.com/beltran/gohive"
	v "github.com/core-go/core/v10"

	"github.com/core-go/hive"
	"github.com/core-go/log"
	query "github.com/core-go/search/hive"
	"github.com/core-go/search/template"
	"github.com/core-go/search/template/xml"

	"go-service/internal/handler"
	"go-service/internal/model"
	"go-service/internal/repository"
	"go-service/internal/service"
)

type ApplicationContext struct {
	User handler.UserPort
}

func NewApp(ctx context.Context, conf Config) (*ApplicationContext, error) {
	configuration := NewConnectConfiguration()
	configuration.Database = "masterdata"
	connection, errConn := Connect(conf.Hive.Host, conf.Hive.Port, conf.Hive.Auth, configuration)
	if errConn != nil {
		return nil, errConn
	}

	templates, err := template.LoadTemplates(xml.Trim, "configs/query.xml")
	if err != nil {
		return nil, err
	}

	logError := log.LogError
	validator := v.NewValidator()

	userType := reflect.TypeOf(model.User{})
	userQuery := query.NewBuilder("users", userType)
	userSearchBuilder, err := hive.NewSearchBuilder(connection, userType, userQuery.BuildQuery)
	if err != nil {
		return nil, err
	}
	userRepository, err := repository.NewUserRepository(connection, templates)
	if err != nil {
		return nil, err
	}
	userService := service.NewUserService(userRepository)
	userHandler := handler.NewUserHandler(userSearchBuilder.Search, userService, validator.Validate, logError)

	return &ApplicationContext{
		User: userHandler,
	}, nil
}
