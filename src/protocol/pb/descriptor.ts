/**
 * GENERATED FILE - do not edit.
 *
 * Source: proto/gate.proto
 * Regenerate: npm run proto:gen
 */
export const gateDescriptor = {
  "nested": {
    "gate": {
      "nested": {
        "v1": {
          "nested": {
            "ClientEnvelope": {
              "oneofs": {
                "body": {
                  "oneof": [
                    "auth",
                    "resume",
                    "heartbeat",
                    "request",
                    "notify"
                  ]
                }
              },
              "fields": {
                "auth": {
                  "type": "Auth",
                  "id": 1
                },
                "resume": {
                  "type": "Resume",
                  "id": 2
                },
                "heartbeat": {
                  "type": "Heartbeat",
                  "id": 3
                },
                "request": {
                  "type": "Request",
                  "id": 4
                },
                "notify": {
                  "type": "Notify",
                  "id": 5
                }
              }
            },
            "Auth": {
              "fields": {
                "token": {
                  "type": "string",
                  "id": 1
                },
                "device": {
                  "type": "string",
                  "id": 2
                }
              }
            },
            "Resume": {
              "fields": {
                "sid": {
                  "type": "string",
                  "id": 1
                },
                "rt": {
                  "type": "string",
                  "id": 2
                },
                "ack": {
                  "type": "uint64",
                  "id": 3
                }
              }
            },
            "Heartbeat": {
              "fields": {
                "ack": {
                  "type": "uint64",
                  "id": 1
                }
              }
            },
            "Request": {
              "fields": {
                "id": {
                  "type": "uint32",
                  "id": 1
                },
                "cmd": {
                  "type": "string",
                  "id": 2
                },
                "d": {
                  "type": "bytes",
                  "id": 3
                },
                "cseq": {
                  "type": "uint64",
                  "id": 4
                },
                "dJson": {
                  "type": "bool",
                  "id": 5
                }
              }
            },
            "Notify": {
              "fields": {
                "cmd": {
                  "type": "string",
                  "id": 1
                },
                "d": {
                  "type": "bytes",
                  "id": 2
                },
                "cseq": {
                  "type": "uint64",
                  "id": 3
                },
                "dJson": {
                  "type": "bool",
                  "id": 4
                }
              }
            },
            "ServerEnvelope": {
              "oneofs": {
                "body": {
                  "oneof": [
                    "authAck",
                    "resumeAck",
                    "heartbeatAck",
                    "response",
                    "push",
                    "kick",
                    "error"
                  ]
                }
              },
              "fields": {
                "authAck": {
                  "type": "AuthAck",
                  "id": 1
                },
                "resumeAck": {
                  "type": "ResumeAck",
                  "id": 2
                },
                "heartbeatAck": {
                  "type": "HeartbeatAck",
                  "id": 3
                },
                "response": {
                  "type": "Response",
                  "id": 4
                },
                "push": {
                  "type": "Push",
                  "id": 5
                },
                "kick": {
                  "type": "Kick",
                  "id": 6
                },
                "error": {
                  "type": "Error",
                  "id": 7
                }
              }
            },
            "AuthAck": {
              "fields": {
                "uid": {
                  "type": "string",
                  "id": 1
                },
                "sid": {
                  "type": "string",
                  "id": 2
                },
                "rt": {
                  "type": "string",
                  "id": 3
                },
                "ts": {
                  "type": "int64",
                  "id": 4
                },
                "rw": {
                  "type": "uint32",
                  "id": 5
                },
                "hb": {
                  "type": "uint32",
                  "id": 6
                }
              }
            },
            "ResumeAck": {
              "fields": {
                "uid": {
                  "type": "string",
                  "id": 1
                },
                "sid": {
                  "type": "string",
                  "id": 2
                },
                "ts": {
                  "type": "int64",
                  "id": 3
                },
                "replay": {
                  "type": "uint32",
                  "id": 4
                },
                "cack": {
                  "type": "uint64",
                  "id": 5
                },
                "seq": {
                  "type": "uint64",
                  "id": 6
                },
                "rt": {
                  "type": "string",
                  "id": 7
                },
                "rw": {
                  "type": "uint32",
                  "id": 8
                },
                "hb": {
                  "type": "uint32",
                  "id": 9
                },
                "resync": {
                  "type": "bool",
                  "id": 10
                },
                "redirect": {
                  "type": "string",
                  "id": 11
                }
              }
            },
            "HeartbeatAck": {
              "fields": {
                "ts": {
                  "type": "int64",
                  "id": 1
                }
              }
            },
            "Response": {
              "fields": {
                "id": {
                  "type": "uint32",
                  "id": 1
                },
                "seq": {
                  "type": "uint64",
                  "id": 2
                },
                "d": {
                  "type": "bytes",
                  "id": 3
                },
                "e": {
                  "type": "uint32",
                  "id": 4
                },
                "m": {
                  "type": "string",
                  "id": 5
                },
                "dJson": {
                  "type": "bool",
                  "id": 6
                }
              }
            },
            "Push": {
              "fields": {
                "seq": {
                  "type": "uint64",
                  "id": 1
                },
                "cmd": {
                  "type": "string",
                  "id": 2
                },
                "d": {
                  "type": "bytes",
                  "id": 3
                },
                "dJson": {
                  "type": "bool",
                  "id": 4
                }
              }
            },
            "Kick": {
              "fields": {
                "reason": {
                  "type": "string",
                  "id": 1
                },
                "m": {
                  "type": "string",
                  "id": 2
                },
                "resumable": {
                  "type": "bool",
                  "id": 3
                }
              }
            },
            "Error": {
              "fields": {
                "e": {
                  "type": "uint32",
                  "id": 1
                },
                "m": {
                  "type": "string",
                  "id": 2
                },
                "id": {
                  "type": "uint32",
                  "id": 3
                }
              }
            }
          }
        }
      }
    }
  }
} as const;
