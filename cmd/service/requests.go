package main

import dbconnector "predixaai-backend"

type baseRequest struct {
	ConnectionRef string                       `json:"connectionRef"`
	Connection    dbconnector.ConnectionConfig `json:"connection"`
}

type tableRequest struct {
	ConnectionRef string                       `json:"connectionRef"`
	Connection    dbconnector.ConnectionConfig `json:"connection"`
	Table         string                       `json:"table"`
	Schema        string                       `json:"schema"`
}

type sampleRequest struct {
	ConnectionRef string                       `json:"connectionRef"`
	Connection    dbconnector.ConnectionConfig `json:"connection"`
	Table         string                       `json:"table"`
	Limit         int                          `json:"limit"`
}

type profileRequest struct {
	ConnectionRef string                       `json:"connectionRef"`
	Connection    dbconnector.ConnectionConfig `json:"connection"`
	Table         string                       `json:"table"`
	Options       dbconnector.ProfileOptions   `json:"options"`
}
