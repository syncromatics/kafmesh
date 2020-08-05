// Code generated by MockGen. DO NOT EDIT.
// Source: ./subscribers.go

// Package subscription_test is a generated GoMock package.
package subscription_test

import (
	context "context"
	gomock "github.com/golang/mock/gomock"
	subscription "github.com/syncromatics/kafmesh/internal/graph/subscription"
	v1 "k8s.io/api/core/v1"
	v10 "k8s.io/apimachinery/pkg/apis/meta/v1"
	reflect "reflect"
)

// MockPodLister is a mock of PodLister interface
type MockPodLister struct {
	ctrl     *gomock.Controller
	recorder *MockPodListerMockRecorder
}

// MockPodListerMockRecorder is the mock recorder for MockPodLister
type MockPodListerMockRecorder struct {
	mock *MockPodLister
}

// NewMockPodLister creates a new mock instance
func NewMockPodLister(ctrl *gomock.Controller) *MockPodLister {
	mock := &MockPodLister{ctrl: ctrl}
	mock.recorder = &MockPodListerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockPodLister) EXPECT() *MockPodListerMockRecorder {
	return m.recorder
}

// List mocks base method
func (m *MockPodLister) List(arg0 context.Context, arg1 v10.ListOptions) (*v1.PodList, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "List", arg0, arg1)
	ret0, _ := ret[0].(*v1.PodList)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// List indicates an expected call of List
func (mr *MockPodListerMockRecorder) List(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "List", reflect.TypeOf((*MockPodLister)(nil).List), arg0, arg1)
}

// MockFactory is a mock of Factory interface
type MockFactory struct {
	ctrl     *gomock.Controller
	recorder *MockFactoryMockRecorder
}

// MockFactoryMockRecorder is the mock recorder for MockFactory
type MockFactoryMockRecorder struct {
	mock *MockFactory
}

// NewMockFactory creates a new mock instance
func NewMockFactory(ctrl *gomock.Controller) *MockFactory {
	mock := &MockFactory{ctrl: ctrl}
	mock.recorder = &MockFactoryMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockFactory) EXPECT() *MockFactoryMockRecorder {
	return m.recorder
}

// Client mocks base method
func (m *MockFactory) Client(ctx context.Context, url string) (subscription.Watcher, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Client", ctx, url)
	ret0, _ := ret[0].(subscription.Watcher)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Client indicates an expected call of Client
func (mr *MockFactoryMockRecorder) Client(ctx, url interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Client", reflect.TypeOf((*MockFactory)(nil).Client), ctx, url)
}
