package main

import (
	"github.com/badoo/thunder/jobgen"
	"fmt"
	"html"
	"io"
	"net/http"
	"sort"
	"time"
)

type ByClassAndLocation []jobgen.DispatcherThreadDescr

func (a ByClassAndLocation) Len() int      { return len(a) }
func (a ByClassAndLocation) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a ByClassAndLocation) Less(i, j int) bool {
	if a[i].ClassName == a[j].ClassName {
		return a[i].Location < a[j].Location
	}
	return a[i].ClassName < a[j].ClassName
}

type ByClass []*jobgen.RunQueueEntry

func (a ByClass) Len() int      { return len(a) }
func (a ByClass) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a ByClass) Less(i, j int) bool {
	if a[i].ClassName == a[j].ClassName {
		return a[i].Id < a[j].Id
	}
	return a[i].ClassName < a[j].ClassName
}

type ByStatusAndClass []*jobgen.RunQueueEntry

func (a ByStatusAndClass) Len() int      { return len(a) }
func (a ByStatusAndClass) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a ByStatusAndClass) Less(i, j int) bool {
	if a[i].RunStatus == a[j].RunStatus {
		return a[i].ClassName < a[j].ClassName
	}
	return a[i].RunStatus < a[j].RunStatus
}

type ByNextLaunchTsAndJobData []*jobgen.TimetableEntry

func (a ByNextLaunchTsAndJobData) Len() int      { return len(a) }
func (a ByNextLaunchTsAndJobData) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a ByNextLaunchTsAndJobData) Less(i, j int) bool {
	if a[i].NextLaunchTs.Int64 == a[j].NextLaunchTs.Int64 {
		return a[i].JobData < a[j].JobData
	}
	return a[i].NextLaunchTs.Int64 < a[j].NextLaunchTs.Int64
}

const STYLE = `<style type="text/css">
	table, tr, td, th { border: 1px black solid; border-collapse: collapse; }
	td { padding: 2px; }
	pre { white-space: pre-wrap; }
	</style>`

func debugPage(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "<title>TH Status</title>")
	fmt.Fprintf(w, "%s", STYLE)
	fmt.Fprintf(w, "<h1>Dispatchers</h1><table><tr><th>class</th><th>location</th><th>&nbsp;</th></tr>")

	res := jobgen.GetDispatcherThreadsList()
	sort.Sort(ByClassAndLocation(res))

	for _, row := range res {
		fmt.Fprintf(w, "<tr><td>%s</td><td>%s</td><td><a href='jobs?class=%s&amp;location=%s'>jobs</a></td></tr>",
			html.EscapeString(row.ClassName), html.EscapeString(row.Location),
			html.EscapeString(row.ClassName), html.EscapeString(row.Location))
	}

	fmt.Fprintf(w, "</table>")
	fmt.Fprintf(w, "<h1>Launchers</h1><table><tr><th>hostname</th><th>&nbsp;</th></tr>")

	lres := jobgen.GetLauncherThreadsList()
	sort.Strings(lres)

	for _, hostname := range lres {
		fmt.Fprintf(w, "<tr><td>%s</td><td><a href='jobs?hostname=%s'>jobs</a></td></tr>",
			html.EscapeString(hostname),
			html.EscapeString(hostname))
	}

	kres := jobgen.GetKillerThreadsList()
	if len(kres) > 0 {
		fmt.Fprintf(w, "</table><h1>Killers</h1><table><tr><th>class</th></tr>")
		sort.Strings(kres)

		for _, className := range kres {
			fmt.Fprintf(w, "<tr><td>%s</td></tr>",
				html.EscapeString(className))
		}
	}

	fmt.Fprintf(w, "</table>")
}

func launcherPrint(w io.Writer, idRows map[uint64]*jobgen.RunQueueEntry, m []*jobgen.RunQueueEntry, status string) {
	sort.Sort(ByClass(m))

	for _, v := range m {
		el := idRows[v.Id]
		dbStatus := " (Missing in DB)"
		if el != nil {
			if el.RunStatus == status {
				dbStatus = ""
			} else {
				dbStatus = " (" + el.RunStatus + " in DB)"
			}
			delete(idRows, v.Id)
		}

		rowStatus := ""
		if rowStatus != status {
			rowStatus = " (" + v.RunStatus + " in actual row)"
		}

		fmt.Fprintf(w, "<tr><td>%d</td><td>%s</td><td>%s</td><td>%s%s</td></tr>", v.Id, html.EscapeString(v.ClassName), html.EscapeString(v.JobData), status, dbStatus)
	}
}

func launcherJobsDebugPage(w io.Writer, hostname string) {
	fmt.Fprintf(w, "<title>TH RQ %s</title>", hostname)

	jobs, err := jobgen.GetLauncherJobs(hostname)
	if err != nil {
		fmt.Fprintf(w, "Error: %s", html.EscapeString(err.Error()))
		return
	}

	allBaseRows, err := jobgen.SelectRunQueue()
	if err != nil {
		fmt.Fprintf(w, "Could not select rows from database: %s", err.Error())
		return
	}

	idRows := make(map[uint64]*jobgen.RunQueueEntry)
	hostRows := allBaseRows[hostname]

	if hostRows != nil {
		sort.Sort(ByStatusAndClass(hostRows))
		for _, row := range hostRows {
			idRows[row.Id] = row
		}
	}

	fmt.Fprintf(w, "<h1>Launcher jobs for hostname=%s</h1><table><tr><th>id</th><th>class name</th><th>job data</th><th>status</th></tr>",
		html.EscapeString(hostname))

	launcherPrint(w, idRows, jobs.Waiting, "Waiting")
	launcherPrint(w, idRows, jobs.Init, "Init")
	launcherPrint(w, idRows, jobs.Running, "Running")
	launcherPrint(w, idRows, jobs.Finished, "Finished")

	if len(idRows) > 0 {
		fmt.Fprintf(w, "</table><h1>Extra rows (present in DB, not present in maps)</h1><table><tr><th>class name</th><th>job data</th><th>status</th></tr>")
		for _, v := range hostRows {
			if idRows[v.Id] == nil {
				continue
			}
			fmt.Fprintf(w, "<tr><td>%s</td><td>%s</td><td>%s</td></tr>", html.EscapeString(v.ClassName), html.EscapeString(v.JobData), v.RunStatus)
		}
	}

	fmt.Fprintf(w, "</table><h1>Raw state:</h1><pre>%s</pre>", jobs.RawResp)
}

func jobsDebugPage(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "%s", STYLE)
	r.ParseForm()

	hostname := r.Form.Get("hostname")
	className := r.Form.Get("class")
	location := r.Form.Get("location")

	if hostname != "" {
		launcherJobsDebugPage(w, hostname)
		return
	}

	if className == "" || location == "" {
		fmt.Fprintf(w, "Error: arguments 'className' and 'location' or 'hostname' are missing")
		return
	}

	fmt.Fprintf(w, "<title>TH TT %s, %s</title>", className, location)

	jobs, err := jobgen.GetDispatcherJobs(className, location)
	if err != nil {
		fmt.Fprintf(w, "Error: %s", html.EscapeString(err.Error()))
		return
	}

	sort.Sort(ByNextLaunchTsAndJobData(jobs.Waiting))
	sort.Sort(ByNextLaunchTsAndJobData(jobs.Added))

	fmt.Fprintf(w, "<h1>Jobs for class=%s and location=%s</h1><table><tr><th>job data</th><th>status</th><th></th></tr>",
		html.EscapeString(className), html.EscapeString(location))

	now := time.Now().Unix()
	for _, v := range jobs.Waiting {
		fmt.Fprintf(w, "<tr><td>%s</td><td>waiting</td><td>next launch in %d seconds</td></tr>", v.JobData, v.NextLaunchTs.Int64-now)
	}

	for _, v := range jobs.Added {
		fmt.Fprintf(w, "<tr><td>%s</td><td>added</td><td></td></tr>", v.JobData)
	}

	fmt.Fprintf(w, "</table><h1>Raw state:</h1><pre>%s</pre>", jobs.RawResp)
}
