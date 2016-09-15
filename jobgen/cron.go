package jobgen

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"badoo/_packages/log"
)

type cronRange struct {
	table map[int]bool
	list  []int
}

var spacePlus = regexp.MustCompile("\\s+")

func getNextLaunchTsForCron(ts, now uint64, secRnd int, cron string) (newTs uint64) {
	parts := spacePlus.Split(strings.TrimSpace(cron), 5)
	if len(parts) != 5 {
		log.Error("Incorrect cron: " + cron)
		return
	}

	minutes := cronMakeRange(parts[0], 0, 59)
	hours := cronMakeRange(parts[1], 0, 23)
	days_of_month := cronMakeRange(parts[2], 1, 31)
	months := cronMakeRange(parts[3], 1, 12)
	days_of_week := cronMakeRange(parts[4], 0, 7)

	newTs = cronNextLaunchTime(minutes, hours, days_of_month, months, days_of_week, now, secRnd)

	if newTs-60 <= ts {
		now += 60
		newTs = cronNextLaunchTime(minutes, hours, days_of_month, months, days_of_week, now, secRnd)
	}

	return
}

/**
 * Compute next launch timestamp based on cron settings
 *
 * @param $minutes array           1. Range for minute column (e.g. self::_cronMakeRange("1-33/2,55", 0, 59))
 * @param $hours array             2. Range for hour column
 * @param $days_of_month array     3. Range for day of month column
 * @param $months array            4. Range for month column
 * @param $days_of_week array      5. Range for day of week column
 * @param $time int                Base timestamp from which to compute next timestamp
 * @return int
 */
func cronNextLaunchTime(minutes, hours, days_of_month, months, days_of_week cronRange, baseTs uint64, secRnd int) uint64 {
	baseDt := time.Unix(int64(baseTs), 0).UTC()

	y := baseDt.Year()
	m := int(baseDt.Month())
	d := -1
	h := -1
	mi := -1

	var overflow bool

	if !months.table[m] {
		m, overflow = cronFindClosestNext(m, months)
		if overflow {
			y++
		}
	} else {
		days := cronGetDays(m, y, days_of_month, days_of_week)
		d = baseDt.Day()

		if !days.table[d] {
			d, overflow = cronFindClosestNext(d, days)
			if overflow {
				m, overflow = cronFindClosestNext(m, months)
				if overflow {
					y++
				}
				d = -1
			}
		} else {
			h = baseDt.Hour()
			if !hours.table[h] {
				h, overflow = cronFindClosestNext(h, hours)
				if overflow {
					d, overflow = cronFindClosestNext(d, days)
					if overflow {
						m, overflow = cronFindClosestNext(m, months)
						if overflow {
							y++
						}
						d = -1
						h = -1
					}
				}
			} else {
				mi = baseDt.Minute()
				if !minutes.table[mi] {
					mi, overflow = cronFindClosestNext(mi, minutes)
					if overflow {
						h, overflow = cronFindClosestNext(h, hours)
						if overflow {
							d, overflow = cronFindClosestNext(d, days)
							if overflow {
								m, overflow = cronFindClosestNext(m, months)
								if overflow {
									y++
								}
								d = -1
								h = -1
								mi = -1
							}
						}
					}
				}
			}
		}
	}

	if d == -1 {
		days := cronGetDays(m, y, days_of_month, days_of_week)
		for _, d = range days.list {
			break
		}
	}

	if h == -1 {
		for _, h = range hours.list {
			break
		}
	}

	if mi == -1 {
		for _, mi = range minutes.list {
			break
		}
	}

	return uint64(time.Date(y, time.Month(m), d, h, mi, secRnd, 0, time.UTC).Unix())
}

func cronFindClosestNext(value int, options cronRange) (res int, overflow bool) {
	for _, key := range options.list {
		if key > value {
			res = key
			return
		}
	}

	overflow = true

	for _, res = range options.list {
		break
	}

	return
}

/**
 * Get array(day => true) of days that match supplied days_of_month or days_of_week for a given month and year
 *
 * @param $month
 * @param $year
 * @param $days_of_month    3. Range for day of month column
 * @param $days_of_week     5. Range for day of week column
 * @return array
 */
func cronGetDays(month, year int, days_of_month_range, days_of_week_range cronRange) (result cronRange) {
	first_day_of_week := int(time.Date(year, time.Month(month), 1, 0, 0, 0, 0, time.UTC).Weekday())
	days_count := time.Date(year, time.Month(month+1), 0, 0, 0, 0, 0, time.UTC).Day()

	result.table = make(map[int]bool)
	result.list = make([]int, 0)

	days_of_week := make(map[int]bool)
	for day := range days_of_week_range.table {
		days_of_week[day] = true
	}

	days_of_month := make(map[int]bool)
	for day := range days_of_month_range.table {
		days_of_month[day] = true
	}

	// Sunday can be set as either 0 or 7 in cron. For our purposes 0 is probably better
	if days_of_week[7] {
		delete(days_of_week, 7)
		days_of_week[0] = true
	}

	days_of_week_skipped := false
	if len(days_of_week) == 7 {
		days_of_week = make(map[int]bool)
		days_of_week_skipped = true
	}

	for day := range days_of_month {
		if day > days_count || day < 1 {
			delete(days_of_month, day)
		}
	}

	days_of_month_skipped := false

	if len(days_of_month) == days_count {
		days_of_month = make(map[int]bool)
		days_of_month_skipped = true
	}

	if days_of_month_skipped && days_of_week_skipped {
		for i := 1; i <= days_count; i++ {
			result.table[i] = true
			result.list = append(result.list, i)
		}
		return
	}

	result.table = days_of_month
	for i := 1; i <= days_count; i++ {
		if days_of_week[(first_day_of_week+i-1)%7] {
			result.table[i] = true
		}
	}

	for i := range result.table {
		result.list = append(result.list, i)
	}

	sort.Ints(result.list)
	return
}

/**
 * Convert "1-23/4" to array(1 => true, 5 => true, 9 => true, 13 => true, 17 => true, 21 => true)
 *
 * @param $col string      crontab column
 * @param $min_value int   minimum value if "*" is supplied (e.g. 0 for hour column, 1 for day of month, etc.)
 * @param $max_value int   maximum value if "*" is supplied (e.g. 23 for hour column, 31 for day of month, etc.)
 * @return array
 */
func cronMakeRange(col string, minValue, maxValue int) (result cronRange) {
	result.table = make(map[int]bool)
	result.list = make([]int, 0)

	for _, rng := range strings.Split(col, ",") {
		parts := strings.Split(rng, "/")
		step := 1
		rng = parts[0]
		if len(parts) > 1 {
			stepParsed, _ := strconv.Atoi(parts[1])
			if stepParsed > 1 {
				step = stepParsed
			}
		}

		if rng == "*" {
			rng = fmt.Sprintf("%d-%d", minValue, maxValue)
		}

		parts = strings.Split(rng, "-")
		var start, end int

		parsedStart, _ := strconv.Atoi(parts[0])
		if parsedStart >= minValue {
			start = parsedStart
			end = parsedStart
		} else {
			start = minValue
			end = minValue
		}

		if len(parts) > 1 {
			endParsed, _ := strconv.Atoi(parts[1])
			if endParsed < maxValue {
				end = endParsed
			} else {
				end = maxValue
			}
		}

		for i := start; i <= end; i += step {
			result.table[i] = true
		}
	}

	for i := range result.table {
		result.list = append(result.list, i)
	}

	sort.Ints(result.list)
	return
}
