package jobgen

import (
	"sort"
	"testing"
	"time"
)

const RAND = 4 // http://xkcd.com/221/

func TestMinutely(t *testing.T) {
	start := uint64(time.Date(2013, 1, 1, 0, 0, 4, 0, time.UTC).Unix())
	cron := "* * * * *"

	prevTs := start
	for i := 0; i < 10; i++ {
		expected := start + uint64(i+1)*60
		now := prevTs + 10

		prevTs = getNextLaunchTsForCron(prevTs, now, RAND, cron)

		if prevTs != expected {
			t.Errorf("Incorrect next launch time for cron on iteration %d: got %d instead of %d", i, prevTs, expected)
			break
		}
	}
}

func TestHourly(t *testing.T) {
	start := uint64(time.Date(2013, 1, 1, 0, 0, 4, 0, time.UTC).Unix())
	cron := "0 * * * *"

	prevTs := start
	for i := 0; i < 10; i++ {
		expected := start + uint64(i+1)*3600
		now := prevTs + 10

		prevTs = getNextLaunchTsForCron(prevTs, now, RAND, cron)

		if prevTs != expected {
			t.Errorf("Incorrect next launch time for cron on iteration %d: got %d instead of %d", i, prevTs, expected)
			break
		}
	}
}

func TestHourly5(t *testing.T) {
	start := uint64(time.Date(2013, 1, 1, 0, 0, 4, 0, time.UTC).Unix())
	cron := "5 * * * *"

	prevTs := start
	for i := 0; i < 10; i++ {
		expected := start + uint64(i)*3600 + 300
		now := prevTs + 10

		prevTs = getNextLaunchTsForCron(prevTs, now, RAND, cron)

		if prevTs != expected {
			t.Errorf("Incorrect next launch time for cron on iteration %d: got %d instead of %d", i, prevTs, expected)
			break
		}
	}
}

func TestEvery5Minutes(t *testing.T) {
	start := uint64(time.Date(2013, 1, 1, 0, 0, 4, 0, time.UTC).Unix())
	cron := "*/5 * * * *"

	prevTs := start
	for i := 0; i < 10; i++ {
		expected := start + uint64(i+1)*300
		now := prevTs + 10

		prevTs = getNextLaunchTsForCron(prevTs, now, RAND, cron)

		if prevTs != expected {
			t.Errorf("Incorrect next launch time for cron on iteration %d: got %d instead of %d", i, prevTs, expected)
			break
		}
	}
}

func TestFizzBuzz(t *testing.T) {
	start := uint64(time.Date(2013, 1, 1, 0, 0, 4, 0, time.UTC).Unix())
	cron := "*/5,*/3 * * * *"

	max := 100
	timesMap := make(map[int]bool)

	for i := 1; i <= max; i++ {
		timesMap[i*3] = true
		timesMap[i*5] = true
	}

	times := make([]int, 0)
	for tm := range timesMap {
		times = append(times, tm)
	}

	sort.Ints(times)

	prevTs := start
	for i := 0; i < max; i++ {
		expected := start + uint64(times[i])*60
		now := prevTs + 10

		prevTs = getNextLaunchTsForCron(prevTs, now, RAND, cron)

		if prevTs != expected {
			t.Errorf("Incorrect next launch time for cron on iteration %d: got %d instead of %d", i, prevTs, expected)
			break
		}
	}
}

func TestCronDaily(t *testing.T) {
	start := uint64(time.Date(2013, 1, 1, 0, 0, 4, 0, time.UTC).Unix())
	cron := "0 0 * * *"

	prevTs := start
	for i := 0; i < 10; i++ {
		expected := start + uint64(i+1)*86400
		now := prevTs + 10

		prevTs = getNextLaunchTsForCron(prevTs, now, RAND, cron)

		if prevTs != expected {
			t.Errorf("Incorrect next launch time for cron on iteration %d: got %d instead of %d", i, prevTs, expected)
			break
		}
	}
}

func TestCronWeekly(t *testing.T) {
	start := uint64(time.Date(2013, 1, 1, 0, 0, 4, 0, time.UTC).Unix())
	cron := "0 0 * * 3"

	expected := start + 86400
	prevTs := start
	for i := 0; i < 15; i++ {
		now := prevTs + 10

		prevTs = getNextLaunchTsForCron(prevTs, now, RAND, cron)

		if prevTs != expected {
			t.Errorf("Incorrect next launch time for cron on iteration %d: got %d instead of %d", i, prevTs, expected)
			break
		}

		expected += 7 * 86400
	}
}

func TestCronMonthly(t *testing.T) {
	start := uint64(time.Date(2013, 1, 1, 0, 0, 4, 0, time.UTC).Unix())
	cron := "0 0 1 * *"

	prevTs := start
	for i := 0; i < 15; i++ {
		expected := uint64(time.Date(2013, time.Month(i+2), 1, 0, 0, 4, 0, time.UTC).Unix())
		now := prevTs + 10

		prevTs = getNextLaunchTsForCron(prevTs, now, RAND, cron)

		if prevTs != expected {
			t.Errorf("Incorrect next launch time for cron on iteration %d: got %d instead of %d", i, prevTs, expected)
			break
		}
	}
}

func TestCronYearly(t *testing.T) {
	start := uint64(time.Date(2013, 1, 1, 0, 0, 4, 0, time.UTC).Unix())
	cron := "0 0 2 1 *"

	prevTs := start
	for i := 0; i < 15; i++ {
		expected := uint64(time.Date(2013+i, 1, 2, 0, 0, 4, 0, time.UTC).Unix())
		now := prevTs + 10

		prevTs = getNextLaunchTsForCron(prevTs, now, RAND, cron)

		if prevTs != expected {
			t.Errorf("Incorrect next launch time for cron on iteration %d: got %d instead of %d", i, prevTs, expected)
			break
		}
	}
}

type doubleDate struct {
	currentDate  string
	expectedDate string
}

func TestCronMonthList(t *testing.T) {
	values := []doubleDate{
		{"2013-01-01 00:00:00", "2013-01-02 00:10:04"},
		{"2013-01-02 00:09:05", "2013-01-02 00:10:04"},
		{"2013-01-02 13:00:00", "2013-03-02 00:10:04"},
		{"2013-01-02 00:00:00", "2013-01-02 00:10:04"},
		{"2013-01-03 00:00:00", "2013-03-02 00:10:04"},
		{"2013-02-02 00:00:00", "2013-03-02 00:10:04"},
		{"2013-03-02 00:00:00", "2013-03-02 00:10:04"},
		{"2013-04-02 00:00:00", "2013-05-02 00:10:04"},
		{"2013-05-02 00:00:00", "2013-05-02 00:10:04"},
		{"2013-06-02 00:00:00", "2013-07-02 00:10:04"},
		{"2013-07-02 00:00:00", "2013-07-02 00:10:04"},
		{"2013-08-02 00:00:00", "2014-01-02 00:10:04"},
		{"2013-09-02 00:00:00", "2014-01-02 00:10:04"},
		{"2013-10-02 00:00:00", "2014-01-02 00:10:04"},
		{"2013-11-02 00:00:00", "2014-01-02 00:10:04"},
		{"2013-12-03 00:00:00", "2014-01-02 00:10:04"},
	}

	cron := "10 0 2 1/2,7,5,3 *"

	for _, dd := range values {
		now, err := time.Parse("2006-01-02 15:04:05", dd.currentDate)
		if err != nil {
			panic("Could not parse " + dd.currentDate + ": " + err.Error())
		}

		expectedTs, err := time.Parse("2006-01-02 15:04:05", dd.expectedDate)
		if err != nil {
			panic("Could not parse " + dd.expectedDate + ": " + err.Error())
		}

		actualTs := getNextLaunchTsForCron(uint64(now.Unix()), uint64(now.Unix()), RAND, cron)

		if uint64(expectedTs.Unix()) != actualTs {
			t.Errorf("Incorrect next launch time for cron on %s - %s: got %d instead of %d", dd.currentDate, dd.expectedDate, actualTs, expectedTs)
			break
		}
	}
}

func TestCronSimultaneous(t *testing.T) {
	values := []doubleDate{
		{"2013-03-01 00:00:00", "2013-05-01 00:10:04"},
		{"2013-05-01 00:00:00", "2013-05-01 00:10:04"},
		{"2013-05-01 00:10:00", "2013-05-02 00:10:04"},
		{"2013-05-02 00:10:00", "2013-05-04 00:10:04"},
		{"2013-05-03 00:10:00", "2013-05-04 00:10:04"},
		{"2013-05-04 00:10:00", "2013-05-05 00:10:04"},
		{"2013-05-05 00:10:00", "2013-05-08 00:10:04"},
		{"2013-05-08 00:10:00", "2013-05-09 00:10:04"},
		{"2013-05-09 00:10:00", "2013-05-11 00:10:04"},
		{"2013-05-11 00:10:00", "2013-05-12 00:10:04"},
		{"2013-05-12 00:10:00", "2013-05-15 00:10:04"},
		{"2013-05-20 00:10:00", "2013-05-21 00:10:04"},
	}

	cron := "10 0 1-5/3,*/10 5,6 */4,*/3"

	for _, dd := range values {
		now, err := time.Parse("2006-01-02 15:04:05", dd.currentDate)
		if err != nil {
			panic("Could not parse " + dd.currentDate + ": " + err.Error())
		}

		expectedTs, err := time.Parse("2006-01-02 15:04:05", dd.expectedDate)
		if err != nil {
			panic("Could not parse " + dd.expectedDate + ": " + err.Error())
		}

		actualTs := getNextLaunchTsForCron(uint64(now.Unix()), uint64(now.Unix()), RAND, cron)

		if uint64(expectedTs.Unix()) != actualTs {
			t.Errorf("Incorrect next launch time for cron on %s - %s: got %d instead of %d", dd.currentDate, dd.expectedDate, actualTs, expectedTs)
			break
		}
	}
}
