// Code generated by MockGen. DO NOT EDIT.
// Source: ./service.go

// Package resolvers_test is a generated GoMock package.
package resolvers_test

import (
	gomock "github.com/golang/mock/gomock"
	model "github.com/syncromatics/kafmesh/internal/graph/model"
	reflect "reflect"
)

// MockServiceLoader is a mock of ServiceLoader interface
type MockServiceLoader struct {
	ctrl     *gomock.Controller
	recorder *MockServiceLoaderMockRecorder
}

// MockServiceLoaderMockRecorder is the mock recorder for MockServiceLoader
type MockServiceLoaderMockRecorder struct {
	mock *MockServiceLoader
}

// NewMockServiceLoader creates a new mock instance
func NewMockServiceLoader(ctrl *gomock.Controller) *MockServiceLoader {
	mock := &MockServiceLoader{ctrl: ctrl}
	mock.recorder = &MockServiceLoaderMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockServiceLoader) EXPECT() *MockServiceLoaderMockRecorder {
	return m.recorder
}

// ComponentsByService mocks base method
func (m *MockServiceLoader) ComponentsByService(arg0 int) ([]*model.Component, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ComponentsByService", arg0)
	ret0, _ := ret[0].([]*model.Component)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ComponentsByService indicates an expected call of ComponentsByService
func (mr *MockServiceLoaderMockRecorder) ComponentsByService(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ComponentsByService", reflect.TypeOf((*MockServiceLoader)(nil).ComponentsByService), arg0)
}