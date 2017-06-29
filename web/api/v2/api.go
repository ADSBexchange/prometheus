// Copyright 2017 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package apiv2

import (
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/net/context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	pb "github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/retrieval"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/tsdb"
	tsdbLabels "github.com/prometheus/tsdb/labels"
)

// API encapsulates all API services.
type API struct {
	now           func() time.Time
	db            *tsdb.DB
	q             func(mint, maxt int64) storage.Querier
	targets       func() []*retrieval.Target
	alertmanagers func() []*url.URL
}

// New returns a new API object.
func New(
	now func() time.Time,
	db *tsdb.DB,
	qe *promql.Engine,
	q func(mint, maxt int64) storage.Querier,
	targets func() []*retrieval.Target,
	alertmanagers func() []*url.URL,
) *API {
	return &API{
		now:           now,
		db:            db,
		q:             q,
		targets:       targets,
		alertmanagers: alertmanagers,
	}
}

// RegisterGRPC registers all API services with the given server.
func (api *API) RegisterGRPC(srv *grpc.Server) {
	pb.RegisterAdminServer(srv, NewAdmin(api.db))
	pb.RegisterStatusServer(srv, NewStatus(api.targets, api.alertmanagers))
	pb.RegisterQueryServer(srv, NewQuery(api.qe, api.q))
}

// HTTPHandler returns an HTTP handler for a REST API gateway to the given grpc address.
func (api *API) HTTPHandler(grpcAddr string) (http.Handler, error) {
	ctx := context.Background()

	enc := new(protoutil.JSONPb)
	mux := runtime.NewServeMux(runtime.WithMarshalerOption(enc.ContentType(), enc))

	opts := []grpc.DialOption{grpc.WithInsecure()}

	err := pb.RegisterAdminHandlerFromEndpoint(ctx, mux, grpcAddr, opts)
	if err != nil {
		return nil, err
	}
	err = pb.RegisterStatusHandlerFromEndpoint(ctx, mux, grpcAddr, opts)
	if err != nil {
		return nil, err
	}
	err = pb.RegisterQueryHandlerFromEndpoint(ctx, mux, grpcAddr, opts)
	if err != nil {
		return nil, err
	}

	return mux, nil
}

type Query struct {
	engine  *promql.Engine
	querier func(mint, maxt int64) storage.Querier
}

func NewQuery(qe *promql.Engine, q func(mint, maxt int64) storage.Querier) *Query {
	return &Query{querier: q, engine: qe}
}

func (s *Query) Instant(ctx context.Context, r *pb.QueryInstantRequest) (*pb.QueryResponse, error) {
	// var (
	// 	mint = timestamp.FromTime(r.Range.MinTime)
	// 	maxt = timestamp.FromTime(r.Range.MaxTime)
	// 	q    = s.querier(mint, maxt)
	// )
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (s *Query) Range(ctx context.Context, r *pb.QueryRangeRequest) (*pb.QueryResponse, error) {
	mint, maxt, err := extractTimeRange(r.Range)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	if r.Step <= 0 {
		return nil, status.Error(codes.InvalidArgument, "positive step size required")
	}
	if maxt.Sub(mint)/r.Step > 11000 {
		return nil, status.Error(codes.InvalidArgument, "maximum resolution exceeded, try decreasing query step size")
	}

	q, err := s.engine.NewRangeQuery(r.Expr, mint, maxt, r.Step)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("invalid query: %s", err))
	}

	if r.Timeout > 0 {
		ctx, _ = context.WithTimeout(ctx, r.Timeout)
	}

	mat, err := q.Exec(ctx).Matrix()

	switch err.(type) {
	case promql.ErrQueryTimeout:
		return nil, status.Error(codes.DeadlineExceeded, err.Error())
	case promql.ErrQueryCanceled:
		return nil, status.Error(codes.Aborted, err.Error())
	case nil:
	default:
		return nil, status.Error(codes.Unknown, err.Error())
	}

	var res pb.QueryResponse

	for _, s := range mat

}

func (s *Query) Label(ctx context.Context, r *pb.LabelRequest) (*pb.LabelResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

// extractTimeRange returns minimum and maximum timestamp in milliseconds as
// provided by the time range. It defaults either boundary to the minimum and maximum
// possible value.
func extractTimeRange(r *pb.TimeRange) (mint, maxt time.Time, err error) {
	if r == nil {
		return minTime, maxTime, nil
	}
	if r.MinTime == nil {
		mint = minTime
	} else {
		mint = *r.MinTime
	}
	if r.MaxTime == nil {
		maxt = maxTime
	} else {
		maxt = *r.MaxTime
	}
	if mint.After(maxt) {
		return mint, maxt, errors.Errorf("min time must be before max time")
	}
	return mint, maxt, nil
}

var (
	minTime = time.Unix(math.MinInt64/1000+62135596801, 0)
	maxTime = time.Unix(math.MaxInt64/1000-62135596801, 999999999)
)

// func (s *Query) Series(ctx context.Context, r *pb.SeriesRequest) (*pb.SeriesResponse, error) {
// 	var (
// 		mint, maxt = extractTimeRange(r.Range)
// 		q          = s.querier(mint, maxt)
// 	)
// 	defer q.Close()

// 	matchers, err := protoToMatchers(r.SelectorOrQuery)
// 	if err != nil {
// 		return nil, status.Error(codes.InvalidArgument, err.Error())
// 	}
// 	set := q.Select(matchers...)

// 	var res pb.SeriesResponse

// 	for set.Next() {
// 		res.Series = append(res.Series, &pb.SeriesInfo{
// 			Labels: labelsToProto(set.At().Labels()),
// 		})
// 	}
// 	if set.Err() != nil {
// 		return nil, status.Error(codes.Internal, set.Err().Error())
// 	}
// 	return &res, nil
// }

type Status struct {
	targets       func() []*retrieval.Target
	alertmanagers func() []*url.URL
}

func NewStatus(
	targets func() []*retrieval.Target,
	alertmanagers func() []*url.URL,
) *Status {
	return &Status{
		targets:       targets,
		alertmanagers: alertmanagers,
	}
}
func protoToMatchers(p []pb.LabelMatcher) ([]*labels.Matcher, error) {
	ms := make([]*labels.Matcher, 0, len(p))

	for _, pm := range p {
		m, err := protoToMatcher(&pm)
		if err != nil {
			return nil, err
		}
		ms = append(ms, m)
	}
	return ms, nil
}

func protoToMatcher(p *pb.LabelMatcher) (lm *labels.Matcher, err error) {
	switch p.Type {
	case pb.LabelMatcher_EQ:
		lm, err = labels.NewMatcher(labels.MatchEqual, p.Name, p.Value)
	case pb.LabelMatcher_NEQ:
		lm, err = labels.NewMatcher(labels.MatchNotEqual, p.Name, p.Value)
	case pb.LabelMatcher_RE:
		lm, err = labels.NewMatcher(labels.MatchRegexp, p.Name, p.Value)
	case pb.LabelMatcher_NRE:
		lm, err = labels.NewMatcher(labels.MatchRegexp, p.Name, p.Value)
	default:
		return nil, errors.Errorf("unknown matcher type")
	}
	return lm, err
}

func labelsToProto(lset labels.Labels) pb.Labels {
	r := pb.Labels{
		Labels: make([]pb.Label, 0, len(lset)),
	}
	for _, l := range lset {
		r.Labels = append(r.Labels, pb.Label{Name: l.Name, Value: l.Value})
	}
	return r
}

func (s *Status) Targets(ctx context.Context, r *pb.TargetsRequest) (*pb.TargetsResponse, error) {
	var res pb.TargetsResponse

	for _, t := range s.targets() {
		pt := &pb.Target{
			ScrapeUrl:        t.URL().String(),
			Labels:           labelsToProto(t.Labels()),
			DiscoveredLabels: labelsToProto(t.DiscoveredLabels()),
			Status: pb.TargetStatus{
				LastScrape: t.LastScrape(),
				LastError:  t.LastError().Error(),
				Health:     pb.TargetStatus_UNKNOWN,
			},
		}
		switch t.Health() {
		case retrieval.HealthGood:
			pt.Status.Health = pb.TargetStatus_HEALTHY
		case retrieval.HealthBad:
			pt.Status.Health = pb.TargetStatus_UNHEALTHY
		}

		res.Targets = append(res.Targets, pt)
	}
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (s *Status) Alertmanagers(ctx context.Context, r *pb.AlertmanagersRequest) (*pb.AlertmanagersResponse, error) {
	var res pb.AlertmanagersResponse

	for _, am := range s.alertmanagers() {
		res.Alertmanagers = append(res.Alertmanagers, &pb.Alertmanager{
			Url: am.String(),
		})
	}
	return &res, status.Error(codes.Unimplemented, "not implemented")
}

func (s *Status) Health(ctx context.Context, _ *pb.HealthRequest) (*pb.HealthResponse, error) {
	return &pb.HealthResponse{}, nil
}

func (s *Status) Readiness(ctx context.Context, _ *pb.ReadinessRequest) (*pb.ReadinessResponse, error) {
	return &pb.ReadinessResponse{}, nil
}

func (s *Status) Config(ctx context.Context, _ *pb.ConfigRequest) (*pb.ConfigResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

// Admin provides an administration interface to Prometheus.
type Admin struct {
	db      *tsdb.DB
	snapdir string
}

// NewAdmin returns a Admin server.
func NewAdmin(db *tsdb.DB) *Admin {
	return &Admin{
		db:      db,
		snapdir: filepath.Join(db.Dir(), "snapshots"),
	}
}

// TSDBReload implements pb.Adminerver.
func (s *Admin) TSDBReload(ctx context.Context, r *pb.TSDBReloadRequest) (*pb.TSDBReloadResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

// TSDBSnapshot implements pb.AdminServer.
func (s *Admin) TSDBSnapshot(_ context.Context, _ *pb.TSDBSnapshotRequest) (*pb.TSDBSnapshotResponse, error) {
	var (
		name = fmt.Sprintf("%s-%x", time.Now().UTC().Format(time.RFC3339), rand.Int())
		dir  = filepath.Join(s.snapdir, name)
	)
	if err := os.MkdirAll(dir, 0777); err != nil {
		return nil, status.Errorf(codes.Internal, "created snapshot directory: %s", err)
	}
	if err := s.db.Snapshot(dir); err != nil {
		return nil, status.Errorf(codes.Internal, "create snapshot: %s", err)
	}
	return &pb.TSDBSnapshotResponse{Name: name}, nil
}

// DeleteSeries imeplements pb.AdminServer.
func (s *Admin) DeleteSeries(_ context.Context, r *pb.SeriesDeleteRequest) (*pb.SeriesDeleteResponse, error) {
	var (
		mint, maxt = extractTimeRange(r.Range)
		matchers   tsdbLabels.Selector
	)
	for _, m := range r.Matchers {
		var lm tsdbLabels.Matcher
		var err error

		switch m.Type {
		case pb.LabelMatcher_EQ:
			lm = tsdbLabels.NewEqualMatcher(m.Name, m.Value)
		case pb.LabelMatcher_NEQ:
			lm = tsdbLabels.Not(tsdbLabels.NewEqualMatcher(m.Name, m.Value))
		case pb.LabelMatcher_RE:
			lm, err = tsdbLabels.NewRegexpMatcher(m.Name, m.Value)
			if err != nil {
				return nil, status.Errorf(codes.InvalidArgument, "bad regexp matcher: %s", err)
			}
		case pb.LabelMatcher_NRE:
			lm, err = tsdbLabels.NewRegexpMatcher(m.Name, m.Value)
			if err != nil {
				return nil, status.Errorf(codes.InvalidArgument, "bad regexp matcher: %s", err)
			}
			lm = tsdbLabels.Not(lm)
		default:
			return nil, status.Error(codes.InvalidArgument, "unknown matcher type")
		}

		matchers = append(matchers, lm)
	}
	if err := s.db.Delete(mint, maxt, matchers...); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pb.SeriesDeleteResponse{}, nil
}
