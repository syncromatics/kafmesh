package models

//DiscoverInfo holds the service and component information for kafmesh-discover to retrieve
type DiscoverInfo struct {
	ServiceName string
	ServiceDescription string
	Component Component
}