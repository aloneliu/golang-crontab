package master

import (
	"net"
	"net/http"
	"time"
)

type ApiServer struct {
	httpServer *http.Server
}

var (
	G_apiServer *ApiServer
)

func InitApiServer() error {
	mux := http.NewServeMux()

	mux.HandleFunc("/job/save", handleJobSave)

	listen, err := net.Listen("tcp", ":8070")
	httpServer := &http.Server{
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
		Handler:      mux,
	}

	G_apiServer = &ApiServer{httpServer: httpServer}

	return err
}

func handleJobSave(w http.ResponseWriter, r *http.Request) {

}
