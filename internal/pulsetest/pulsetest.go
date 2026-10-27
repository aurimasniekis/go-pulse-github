// Package pulsetest boots an embedded nats-server configured the way Pulse configures NATS: an operator, a system
// account, an APP account with JetStream, the EVENTS stream (Pulse's streams.EventsConfig) and least-privilege client
// credentials whose permissions mirror Pulse's natsauth.Permissions.
//
// Pulse's own test harness lives in internal packages that other modules cannot import, so the parts this service
// depends on are reproduced here from github.com/aurimasniekis/go-pulse v0.1.3 (commit 9aba53b5c8cf):
// internal/natsauth/grants.go (Permissions, fixedPubDenies) and internal/streams/events.go (EventsConfig). If Pulse
// changes them, update this file and the pinned version together.
package pulsetest

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nkeys"
)

// Env is a running Pulse-compatible NATS.
type Env struct {
	t        testing.TB
	URL      string
	port     int
	storeDir string
	opJWT    string
	sysPub   string
	accts    map[string]string
	app      nkeys.KeyPair
	srv      *server.Server
}

// Options tune the environment.
type Options struct {
	// DuplicateWindow overrides the EVENTS duplicate window (Pulse uses 2 minutes).
	DuplicateWindow time.Duration
}

// Start boots the server and creates EVENTS and the blobs bucket.
func Start(t testing.TB, o Options) *Env {
	t.Helper()
	op, _ := nkeys.CreateOperator()
	opPub, _ := op.PublicKey()
	oc := jwt.NewOperatorClaims(opPub)
	oc.Name = "pulse-test"
	sys, _ := nkeys.CreateAccount()
	sysPub, _ := sys.PublicKey()
	oc.SystemAccount = sysPub
	opJWT, err := oc.Encode(op)
	if err != nil {
		t.Fatal(err)
	}
	app, _ := nkeys.CreateAccount()
	appPub, _ := app.PublicKey()
	ac := jwt.NewAccountClaims(appPub)
	ac.Name = "APP"
	ac.Limits.JetStreamLimits = jwt.JetStreamLimits{MemoryStorage: -1, DiskStorage: -1, Streams: -1, Consumer: -1}
	appJWT, err := ac.Encode(op)
	if err != nil {
		t.Fatal(err)
	}
	sc := jwt.NewAccountClaims(sysPub)
	sc.Name = "SYS"
	sysJWT, err := sc.Encode(op)
	if err != nil {
		t.Fatal(err)
	}
	e := &Env{t: t, port: freePort(t), storeDir: t.TempDir(), opJWT: opJWT, sysPub: sysPub, app: app,
		accts: map[string]string{appPub: appJWT, sysPub: sysJWT}}
	e.URL = fmt.Sprintf("nats://127.0.0.1:%d", e.port)
	e.boot()
	t.Cleanup(e.Shutdown)

	nc := e.adminConn()
	defer nc.Close()
	js, _ := jetstream.New(nc)
	ctx := context.Background()
	dup := 2 * time.Minute
	if o.DuplicateWindow > 0 {
		dup = o.DuplicateWindow
	}
	// streams.EventsConfig from Pulse v0.1.3 (max_bytes reduced for tests).
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name: "EVENTS", Subjects: []string{"evt.>"}, Retention: jetstream.LimitsPolicy, Storage: jetstream.FileStorage,
		MaxMsgs: -1, MaxBytes: 256 << 20, MaxMsgSize: 1 << 20, MaxMsgsPerSubject: -1, Discard: jetstream.DiscardOld,
		Compression: jetstream.S2Compression, Duplicates: dup, AllowDirect: true, Replicas: 1, MaxConsumers: 256,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: "blobs", Storage: jetstream.FileStorage, Compression: true}); err != nil {
		t.Fatal(err)
	}
	return e
}

func freePort(t testing.TB) int {
	t.Helper()
	var lc net.ListenConfig
	l, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func (e *Env) boot() {
	e.t.Helper()
	oc, err := jwt.DecodeOperatorClaims(e.opJWT)
	if err != nil {
		e.t.Fatal(err)
	}
	res := &server.MemAccResolver{}
	for pub, j := range e.accts {
		if err := res.Store(pub, j); err != nil {
			e.t.Fatal(err)
		}
	}
	opts := &server.Options{
		Host: "127.0.0.1", Port: e.port, JetStream: true, StoreDir: e.storeDir, NoLog: true, NoSigs: true,
		TrustedOperators: []*jwt.OperatorClaims{oc}, AccountResolver: res, SystemAccount: e.sysPub,
		MaxPayload: 1 << 20,
	}
	srv, err := server.NewServer(opts)
	if err != nil {
		e.t.Fatal(err)
	}
	srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		e.t.Fatal("nats-server not ready")
	}
	e.srv = srv
}

// Shutdown stops the server (simulates a Pulse/NATS outage).
func (e *Env) Shutdown() {
	if e.srv != nil {
		e.srv.Shutdown()
		e.srv.WaitForShutdown()
		e.srv = nil
	}
}

// Restart starts the server again on the same port and storage.
func (e *Env) Restart() { e.boot() }

// Grants selects client permissions.
type Grants struct {
	Publish []string
	Blobs   bool
}

// Creds mints a client .creds file (like `pulse clients create`) and returns its path and client ID.
func (e *Env) Creds(clientID string, g Grants) (string, string) {
	e.t.Helper()
	ukp, _ := nkeys.CreateUser()
	upub, _ := ukp.PublicKey()
	uc := jwt.NewUserClaims(upub)
	uc.Name = clientID
	uc.Tags.Add("pulse-client:" + clientID)
	uc.Permissions = Permissions(clientID, g)
	ujwt, err := uc.Encode(e.app)
	if err != nil {
		e.t.Fatal(err)
	}
	seed, _ := ukp.Seed()
	creds, err := jwt.FormatUserConfig(ujwt, seed)
	if err != nil {
		e.t.Fatal(err)
	}
	p := filepath.Join(e.t.TempDir(), clientID+".creds")
	if err := os.WriteFile(p, creds, 0o600); err != nil {
		e.t.Fatal(err)
	}
	return p, clientID
}

// Permissions mirrors natsauth.Permissions (Pulse v0.1.3) for publish and blobs grants.
func Permissions(clientID string, g Grants) jwt.Permissions {
	pub := slices.Clone(g.Publish)
	deny := []string{
		"$SYS.>", "$KV.>", "$JS.API.STREAM.CREATE.>", "$JS.API.STREAM.UPDATE.>", "$JS.API.STREAM.DELETE.>",
		"$JS.API.STREAM.PURGE.>", "$JS.API.STREAM.MSG.DELETE.>", "$JS.API.STREAM.MSG.GET.>", "$JS.API.STREAM.SNAPSHOT.>",
		"$JS.API.STREAM.RESTORE.>", "$JS.API.STREAM.PEER.>", "$JS.API.STREAM.LEADER.>", "$JS.API.ACCOUNT.>",
		"$JS.API.SERVER.>", "$JS.API.META.>", "$JS.API.CONSUMER.DURABLE.CREATE.>", "$JS.API.DIRECT.GET.EVENTS",
	}
	if g.Blobs {
		obj := "OBJ_blobs"
		pub = append(pub, "$JS.API.INFO", "$JS.API.STREAM.INFO."+obj, "$O.blobs.>",
			"$JS.API.CONSUMER.CREATE."+obj+".*.$O.blobs.>", "$JS.API.CONSUMER.INFO."+obj+".*",
			"$JS.API.CONSUMER.MSG.NEXT."+obj+".*", "$JS.API.DIRECT.GET."+obj+".$O.blobs.>", "$JS.ACK."+obj+".>",
			"$JS.FC."+obj+".>")
	} else {
		deny = append(deny, "$O.>")
	}
	if len(pub) == 0 {
		pub = []string{"_pulse.no_publish_grants"}
	}
	slices.Sort(pub)
	slices.Sort(deny)
	return jwt.Permissions{
		Pub: jwt.Permission{Allow: slices.Compact(pub), Deny: slices.Compact(deny)},
		Sub: jwt.Permission{Allow: []string{"_INBOX_" + clientID + ".>"}, Deny: []string{"$SYS.>", "$KV.>"}},
	}
}

// adminConn connects with an unrestricted APP-account user (stands in for pulsed itself).
func (e *Env) adminConn() *nats.Conn {
	e.t.Helper()
	ukp, _ := nkeys.CreateUser()
	upub, _ := ukp.PublicKey()
	ujwt, err := jwt.NewUserClaims(upub).Encode(e.app)
	if err != nil {
		e.t.Fatal(err)
	}
	seed, _ := ukp.Seed()
	nc, err := nats.Connect(e.URL, nats.UserJWTAndSeed(ujwt, string(seed)))
	if err != nil {
		e.t.Fatal(err)
	}
	return nc
}

// Stored reads every message on EVENTS matching filter (admin view).
func (e *Env) Stored(filter string) []*nats.RawStreamMsg {
	e.t.Helper()
	nc := e.adminConn()
	defer nc.Close()
	js, _ := jetstream.New(nc)
	ctx := context.Background()
	s, err := js.Stream(ctx, "EVENTS")
	if err != nil {
		e.t.Fatal(err)
	}
	info, err := s.Info(ctx)
	if err != nil {
		e.t.Fatal(err)
	}
	var out []*nats.RawStreamMsg
	for seq := info.State.FirstSeq; seq <= info.State.LastSeq && info.State.Msgs > 0; seq++ {
		m, err := s.GetMsg(ctx, seq)
		if err != nil {
			continue
		}
		if filter == "" || subjectMatch(filter, m.Subject) {
			out = append(out, &nats.RawStreamMsg{Subject: m.Subject, Sequence: m.Sequence, Header: m.Header, Data: m.Data})
		}
	}
	return out
}

func subjectMatch(filter, subject string) bool {
	return filter == ">" || len(subject) >= len(filter)-1 && subject[:len(filter)-1] == filter[:len(filter)-1]
}
