package model

import "github.com/core-go/search"

type UserFilter struct {
	*search.Filter
	Id          string            `yaml:"id" mapstructure:"id" json:"id" gorm:"column:id;primary_key" bson:"_id" dynamodbav:"id" firestore:"-" avro:"id" validate:"required,max=40" operator:"="`
	Username    string            `yaml:"username" mapstructure:"username" json:"username" gorm:"column:username" bson:"username" dynamodbav:"username" firestore:"username" avro:"username" validate:"required,username,max=100"`
	Email       string            `yaml:"email" mapstructure:"email" json:"email" gorm:"column:email" bson:"email" dynamodbav:"email" firestore:"email" avro:"email" validate:"email,max=100"`
	Phone       string            `yaml:"phone" mapstructure:"phone" json:"phone" gorm:"column:phone" bson:"phone" dynamodbav:"phone" firestore:"phone" avro:"phone" validate:"required,phone,max=18" operator:"like"`
	CreatedDate *search.TimeRange `yaml:"created_date" mapstructure:"created_date" json:"createdDate" gorm:"column:created_date" bson:"createdDate" length:"10" format:"dateFormat:2006-01-02" dynamodbav:"createdDate" firestore:"createdDate"`
}
