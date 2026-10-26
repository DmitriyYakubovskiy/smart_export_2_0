package main

import (
 "fmt"
 "net/http"
 "net/http/httptest"
 "testing"
)

// R5: HTTP 202 is an acknowledgement only with a valid queued response.
// These are stronger acceptance criteria, not an implementation-mirroring test.
func TestR5RejectInvalidQueueAcknowledgement(t *testing.T) {
 cases:=[]struct{name,body string}{
  {"invalid_json",`not json`},
  {"error_status",`{"status":"error","document_id":"0123456789abcdef0123456789abcdef"}`},
  {"missing_document_id",`{"status":"queued"}`},
 }
 for _,tc:=range cases {t.Run(tc.name,func(t *testing.T){
  server:=httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter,r *http.Request){
   w.Header().Set("Content-Type","application/json")
   w.WriteHeader(http.StatusAccepted);fmt.Fprint(w,tc.body)
  }));defer server.Close()
  path:=testFile(t,"valid.pdf",[]byte("%PDF-test"))
  if err:=sendFile(testClient(),server.URL,path);err==nil {
   t.Fatalf("invalid acknowledgement accepted as success: %s",tc.body)
  }
 })}
}
func TestControlValidQueueAcknowledgement(t *testing.T) {
 server:=httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter,r *http.Request){
  w.Header().Set("Content-Type","application/json");w.WriteHeader(http.StatusAccepted)
  fmt.Fprint(w,`{"status":"queued","document_id":"0123456789abcdef0123456789abcdef","filename":"valid.pdf"}`)
 }));defer server.Close()
 if err:=sendFile(testClient(),server.URL,testFile(t,"valid.pdf",[]byte("%PDF-test")));err!=nil {t.Fatal(err)}
}
