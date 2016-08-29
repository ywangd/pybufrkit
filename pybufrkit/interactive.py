from __future__ import absolute_import
import cmd

__all__ = ['Shell']


class Shell(cmd.Cmd, object):

    def __init__(self, vm):
        cmd.Cmd.__init__(self)
        self.vm = vm
        self.prompt = '({}) '.format(vm.__class__.__name__.lower())

    def do_exit(self, arg):
        return True

    def default(self, line):
        if line == 'EOF':
            return True
        else:
            super(Shell, self).default(line)


