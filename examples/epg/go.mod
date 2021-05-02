module epg

go 1.16

require (
	github.com/lib/pq v1.10.1
	github.com/zzztttkkk/sqlx v0.0.0-20210502073307-1ed8a78f56cc
)

replace (
	github.com/zzztttkkk/sqlx v0.0.0-20210502073307-1ed8a78f56cc => ../..
)