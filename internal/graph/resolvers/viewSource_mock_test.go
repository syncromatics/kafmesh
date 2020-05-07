// Code generated by MockGen. DO NOT EDIT.
// Source: ./viewSource.go

// Package resolvers_test is a generated GoMock package.
package resolvers_test

import (
	gomock "github.com/golang/mock/gomock"
	model "github.com/syncromatics/kafmesh/internal/graph/model"
	reflect "reflect"
)

// MockViewSourceLoader is a mock of ViewSourceLoader interface
type MockViewSourceLoader struct {
	ctrl     *gomock.Controller
	recorder *MockViewSourceLoaderMockRecorder
}

// MockViewSourceLoaderMockRecorder is the mock recorder for MockViewSourceLoader
type MockViewSourceLoaderMockRecorder struct {
	mock *MockViewSourceLoader
}

// NewMockViewSourceLoader creates a new mock instance
func NewMockViewSourceLoader(ctrl *gomock.Controller) *MockViewSourceLoader {
	mock := &MockViewSourceLoader{ctrl: ctrl}
	mock.recorder = &MockViewSourceLoaderMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockViewSourceLoader) EXPECT() *MockViewSourceLoaderMockRecorder {
	return m.recorder
}

// ComponentByViewSource mocks base method
func (m *MockViewSourceLoader) ComponentByViewSource(arg0 int) (*model.Component, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ComponentByViewSource", arg0)
	ret0, _ := ret[0].(*model.Component)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ComponentByViewSource indicates an expected call of ComponentByViewSource
func (mr *MockViewSourceLoaderMockRecorder) ComponentByViewSource(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ComponentByViewSource", reflect.TypeOf((*MockViewSourceLoader)(nil).ComponentByViewSource), arg0)
}

// PodsByViewSource mocks base method
func (m *MockViewSourceLoader) PodsByViewSource(arg0 int) ([]*model.Pod, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "PodsByViewSource", arg0)
	ret0, _ := ret[0].([]*model.Pod)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// PodsByViewSource indicates an expected call of PodsByViewSource
func (mr *MockViewSourceLoaderMockRecorder) PodsByViewSource(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "PodsByViewSource", reflect.TypeOf((*MockViewSourceLoader)(nil).PodsByViewSource), arg0)
}

// TopicByViewSource mocks base method
func (m *MockViewSourceLoader) TopicByViewSource(arg0 int) (*model.Topic, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "TopicByViewSource", arg0)
	ret0, _ := ret[0].(*model.Topic)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// TopicByViewSource indicates an expected call of TopicByViewSource
func (mr *MockViewSourceLoaderMockRecorder) TopicByViewSource(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "TopicByViewSource", reflect.TypeOf((*MockViewSourceLoader)(nil).TopicByViewSource), arg0)
}
