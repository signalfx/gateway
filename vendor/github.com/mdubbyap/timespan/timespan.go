//Package timespan provides functionality for handling intervals of time.
package timespan

import (
	"time"
)

//Span represents an inclusive range between two time instants.
//
//The zero value of type span has both start and end times set to the zero value
//of type Time. The zero value is returned by the Intersection and Gap methods
//when there is no span fitting their purposes.
type Span struct {
	start, end time.Time
}

//New
func NewTimes(t1, t2 time.Time) Span {
	start := t1
	end := t2
	if end.Before(start) {
		start, end = end, start
	}

	return Span{
		start: start,
		end:   end,
	}
}

//New creates a new span with the given start instant and duration.
func New(t time.Time, d time.Duration) Span {
	start := t
	end := t.Add(d)
	if end.Before(t) {
		start, end = end, start
	}

	return Span{
		start: start,
		end:   end,
	}
}

//Start returns the time instant at the start of s.
func (s Span) Start() time.Time {
	return s.start
}

//End returns the time instant at the end of s.
func (s Span) End() time.Time {
	return s.end
}

//Duration returns the length of time represented by s.
func (s Span) Duration() time.Duration {
	return s.end.Sub(s.start)
}

//After reports whether s begins after t.
func (s Span) After(t time.Time) bool {
	return s.start.After(t)
}

//Before reports whether s ends before t.
func (s Span) Before(t time.Time) bool {
	return s.end.Before(t)
}

//Borders reports whether s and r are contiguous time intervals.
func (s Span) Borders(r Span) bool {
	return s.start.Equal(r.end) || s.end.Equal(r.start)
}

//ContainsTime reports whether t is within s.
func (s Span) ContainsTime(t time.Time) bool {
	return !(t.Before(s.start) || t.After(s.end))
}

//Contains reports whether r is entirely within s.
func (s Span) Contains(r Span) bool {
	return s.ContainsTime(r.start) && s.ContainsTime(r.end)
}

//Encompass returns the minimum span that fully contains both r and s.
func (s Span) Encompass(r Span) Span {
	return Span{
		start: tmin(s.start, r.start),
		end:   tmax(s.end, r.end),
	}
}

//Equal reports whether s and r represent the same time intervals, ignoring
//the locations of the times.
func (s Span) Equal(r Span) bool {
	return s.start.Equal(r.start) && s.end.Equal(r.end)
}

//Follows reports whether s begins after or at the end of r.
func (s Span) Follows(r Span) bool {
	return !s.start.Before(r.end)
}

//Gap returns a span corresponding to the period between s and r.
//If s and r have a non-zero overlap, a zero span is returned.
func (s Span) Gap(r Span) Span {
	if s.Overlaps(r) {
		return Span{}
	}
	return Span{
		start: tmin(s.end, r.end),
		end:   tmax(s.start, r.start),
	}
}

//Intersection returns both a span corresponding to the non-zero overlap of
//s and r and a bool indicating whether such an overlap existed.
//If s and r do not overlap, a zero span is returned with false.
func (s Span) Intersection(r Span) (Span, bool) {
	if !s.Overlaps(r) {
		return Span{}, false
	}
	return Span{
		start: tmax(s.start, r.start),
		end:   tmin(s.end, r.end),
	}, true
}

//IsZero reports whether s represents the zero-length span starting and ending
//on January 1, year 1, 00:00:00 UTC.
func (s Span) IsZero() bool {
	return s.start.IsZero() && s.end.IsZero()
}

//Offset returns s with its start time offset by d. It is equivalent to
//Newspan(s.Start().Add(d), s.Duration()).
func (s Span) Offset(d time.Duration) Span {
	return Span{
		start: s.start.Add(d),
		end:   s.end.Add(d),
	}
}

//OffsetDate returns s with its start time offset by the given years, months,
//and days. It is equivalent to
//Newspan(s.Start().AddDate(years, months, days), s.Duration()).
func (s Span) OffsetDate(years, months, days int) Span {
	d := s.Duration()
	t := s.start.AddDate(years, months, days)
	return Span{
		start: t,
		end:   t.Add(d),
	}
}

//Overlaps reports whether s and r intersect for a non-zero duration.
func (s Span) Overlaps(r Span) bool {
	return s.start.Before(r.end) && s.end.After(r.start)
}

//Precedes reports whether s ends before or at the start of r.
func (s Span) Precedes(r Span) bool {
	return !s.end.After(r.start)
}

//tmax returns the later of two time instants.
func tmax(t, u time.Time) time.Time {
	if t.After(u) {
		return t
	}
	return u
}

//tmin returns the earlier of two time instants.
func tmin(t, u time.Time) time.Time {
	if t.Before(u) {
		return t
	}
	return u
}
