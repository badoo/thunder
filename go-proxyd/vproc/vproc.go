package vproc

import (
	"badoo/_packages/log"
	"os"
	"os/exec"
)

type Vproc struct {
	Cmd          *exec.Cmd
	Ps           *os.ProcessState
	Id           uint64
	SendResponse bool
}

func NewProcess(id uint64, bin string, cmd string, args []string, stdout_file, stderr_file string) (*Vproc, error) {
	p := new(Vproc)
	p.Id = id
	p.Cmd = exec.Command(bin, cmd)
	var a = []string{bin, cmd}
	a = append(a, args...)

	p.Cmd.Args = a
	p.Cmd.Env = os.Environ()

	log.Debug("%v\n", a)

	stdout, err := os.OpenFile(stdout_file, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0666)
	if err != nil {
		return nil, err
	}
	p.Cmd.Stdout = stdout

	stderr, err := os.OpenFile(stderr_file, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0666)
	if err != nil {
		return nil, err
	}
	p.Cmd.Stderr = stderr

	return p, nil
}

func (vp *Vproc) Start() error {
	err := vp.Cmd.Start()
	if err != nil {
		log.Errorf("%v\n", err)
	}

	return err
}

func (vp *Vproc) Wait() (*os.ProcessState, error) {
	ps, err := vp.Cmd.Process.Wait()
	if err != nil {
		log.Errorf("%v\n", err)
		return ps, err
	}
	return ps, err
}

func (vp *Vproc) Kill() error {
	return vp.Cmd.Process.Kill()
}

func (vp *Vproc) Size() int {
	return 1
}
