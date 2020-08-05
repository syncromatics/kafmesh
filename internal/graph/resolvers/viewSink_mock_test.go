// Code generated by MockGen. DO NOT EDIT.
// Source: ./viewSink.go

// Package resolvers_test is a generated GoMock package.
package resolvers_test

import (
	gomock "github.com/golang/mock/gomock"
	model "github.com/syncromatics/kafmesh/internal/graph/model"
	reflect "reflect"
)

// MockViewSinkLoader is a mock of ViewSinkLoader interface
type MockViewSinkLoader struct {
	ctrl     *gomock.Controller
	recorder *MockViewSinkLoaderMockRecorder
}

// MockViewSinkLoaderMockRecorder is the mock recorder for MockViewSinkLoader
type MockViewSinkLoaderMockRecorder struct {
	mock *MockViewSinkLoader
}

// NewMockViewSinkLoader creates a new mock instance
func NewMockViewSinkLoader(ctrl *gomock.Controller) *MockViewSinkLoader {
	mock := &MockViewSinkLoader{ctrl: ctrl}
	mock.recorder = &MockViewSinkLoaderMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockViewSinkLoader) EXPECT() *MockViewSinkLoaderMockRecorder {
	return m.recorder
}

// ComponentByViewSink mocks base method
func (m *MockViewSinkLoader) ComponentByViewSink(arg0 int) (*model.Component, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ComponentByViewSink", arg0)
	ret0, _ := ret[0].(*model.Component)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ComponentByViewSink indicates an expected call of ComponentByViewSink
func (mr *MockViewSinkLoaderMockRecorder) ComponentByViewSink(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ComponentByViewSink", reflect.TypeOf((*MockViewSinkLoader)(nil).ComponentByViewSink), arg0)
}

// PodsByViewSink mocks base method
func (m *MockViewSinkLoader) PodsByViewSink(arg0 int) ([]*model.Pod, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "PodsByViewSink", arg0)
	ret0, _ := ret[0].([]*model.Pod)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// PodsByViewSink indicates an expected call of PodsByViewSink
func (mr *MockViewSinkLoaderMockRecorder) PodsByViewSink(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "PodsByViewSink", reflect.TypeOf((*MockViewSinkLoader)(nil).PodsByViewSink), arg0)
}

// TopicByViewSink mocks base method
func (m *MockViewSinkLoader) TopicByViewSink(arg0 int) (*model.Topic, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "TopicByViewSink", arg0)
	ret0, _ := ret[0].(*model.Topic)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// TopicByViewSink indicates an expected call of TopicByViewSink
func (mr *MockViewSinkLoaderMockRecorder) TopicByViewSink(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "TopicByViewSink", reflect.TypeOf((*MockViewSinkLoader)(nil).TopicByViewSink), arg0)
}
