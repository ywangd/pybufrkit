"""
BUFR Processing Command Language

Syntax:

string :: This should allow variable interpolation
number :: integer_number | float_number | scientific_number
name ::


file_input = (stmt NEWLINE+)*

stmt :: simple_stmt | compound_stmt

compound_stmt :: if_stmt | for_stmt

for_stmt :: 'for' expr 'in' test suite

if_stmt :: 'if' test suite ('elif' test suite)* ['else' suite]

suite :: '{' NEWLINE* (stmt NEWLINE+)* [stmt NEWLINE*] '}'

simple_stmt :: assert_stmt | print_stmt | expr_stmt

assert_stmt :: 'assert' test

print_stmt :: 'print' [ test (',' test)* [','] ]

expr_stmt :: test + ('=' test)*

test :: or_test ['?' or_test ':' test]
or_test :: and_test ('or' and_test)*
and_test :: not_test ('and' not_test)*
not_test :: 'not' not_test | comparison

comparison :: expr (comp_op expr)*
comp_op :: '<' | '>' | '==' | '>=' | '<=' | '<>' | '!='
expr :: xor_expr ('|' xor_expr)*
xor_expr :: and_expr ('^' and_expr)*
and_expr :: shift_expr ('&' shift_expr)*

shift_expr :: arith_expr ( ('<<' | '>>')  arith_expr)*
arith_expr :: term ( ('+' | '-')  term)*
term :: factor ( ('*' | '/' | '%')  factor)*
factor :: ('+' | '-' | '~') factor | power
power :: atom trailer* ['**' factor]
atom :: name |
        number |
        string |
        '(' test ')'

trailer :: '(' [arglist] ')' | '[' subscript ']' | '.' name

subscript :: test | [test] ':' [test] [sliceop]
sliceop :: ':' [test]

arglist :: (argument ',')* (argument [','])
argument :: test | name '=' test


"""
from __future__ import absolute_import
from __future__ import print_function
import ast
import functools
from six import PY3, text_type

import pyparsing as pp

__all__ = ['bpcl_compiler']


def set_lineno_col(func):
    @functools.wraps(func)
    def wrapped(s, loc, tokens):
        node = func(s, loc, tokens)
        if isinstance(node, ast.AST):
            node.lineno, node.col_offset = pp.lineno(loc, s), pp.col(loc, s)
        return node

    return wrapped


def passby_action(s, loc, tokens):
    return tokens[:]


@set_lineno_col
def number_action(s, loc, tokens):
    value = ast.literal_eval(tokens[0])
    return ast.Num(n=value)


@set_lineno_col
def string_action(s, loc, tokens):
    value = ast.literal_eval(tokens[0])
    if PY3:
        if isinstance(value, text_type):
            return ast.Str(s=value)
        else:
            return ast.Bytes(s=value)
    else:
        return ast.Str(s=value)


@set_lineno_col
def name_action(s, loc, tokens):
    value = tokens[0]
    return ast.Name(id=value, ctx=ast.Load())


def arglist_action(s, loc, tokens):
    args = []
    keywords = []

    for argument in tokens:
        if isinstance(argument, ast.keyword):
            keywords.append(argument)
        else:
            args.append(argument)

    return [args, keywords]


@set_lineno_col
def sliceop_action(s, loc, tokens):
    if len(tokens) == 0:
        return ast.Name(id='None', ctx=ast.Load())
    else:
        return tokens[0]


@set_lineno_col
def subscript_action(s, loc, tokens):
    if 'slice' in tokens:
        slice = tokens.slice
        start = slice.start if 'start' in slice else None
        stop = slice.stop if 'stop' in slice else None
        if 'step' in slice:
            step = slice.step[0]
        else:
            step = None
        return ast.Subscript(slice=ast.Slice(lower=start, upper=stop, step=step), ctx=ast.Load())

    else:
        return ast.Subscript(slice=ast.Index(value=tokens[0]), ctx=ast.Load())


@set_lineno_col
def trailer_action(s, loc, tokens):
    if 'func_call_trailer' in tokens:
        args, keywords = ([], []) if len(tokens) == 0 else (tokens[0], tokens[1])
        return ast.Call(args=args, keywords=keywords, starargs=None, kwargs=None)
    elif 'subscript_trailer' in tokens:
        return tokens[0]
    else:  # attribute
        return ast.Attribute(attr=tokens[0].id, ctx=ast.Load())


@set_lineno_col
def power_action(s, loc, tokens):
    node = tokens[0]

    for trailer in tokens.get('trailers', ()):
        if isinstance(trailer, ast.Call):
            trailer.func = node
        elif isinstance(trailer, ast.Subscript):
            trailer.value = node
        else:  # dot access
            trailer.value = node

        node = trailer

    if 'exponential' in tokens:
        node = ast.BinOp(left=node, op=ast.Pow(), right=tokens['exponential'][0])

    return node


@set_lineno_col
def unary_action(s, loc, tokens):
    op_char, operand = tokens[0], tokens[1]
    if op_char == '+':
        return operand
    elif op_char == '-':
        return ast.UnaryOp(op=ast.USub(), operand=operand)
    elif op_char == '~':
        return ast.UnaryOp(op=ast.Invert(), operand=operand)
    else:  # not
        return ast.UnaryOp(op=ast.Not(), operand=operand)


@set_lineno_col
def factor_action(s, loc, tokens):
    node = tokens[0]
    return node


@set_lineno_col
def kwarg_action(s, loc, tokens):
    target = tokens[0]
    return ast.keyword(arg=target.id, value=tokens[1])


@set_lineno_col
def binop_action(s, loc, tokens):
    node = tokens[0]
    for op_char, right in tokens[1:]:
        if op_char == '+':
            op = ast.Add()
        elif op_char == '-':
            op = ast.Sub()
        elif op_char == '*':
            op = ast.Mult()
        elif op_char == '/':
            op = ast.Div()
        elif op_char == '%':
            op = ast.Mod()
        elif op_char == '<<':
            op = ast.LShift()
        elif op_char == '>>':
            op = ast.RShift()
        elif op_char == '&':
            op = ast.BitAnd()
        elif op_char == '^':
            op = ast.BitXor()
        else:  # op_char == '|':
            op = ast.BitOr()

        node = ast.BinOp(left=node, op=op, right=right,
                         lineno=1, col_offset=0)

    return node


@set_lineno_col
def compare_action(s, loc, tokens):
    node = tokens[0]
    ops = []
    comparators = []
    for op_char, right in tokens[1:]:
        if op_char == '==':
            ops.append(ast.Eq())
        elif op_char == '<':
            ops.append(ast.Lt())
        elif op_char == '<=':
            ops.append(ast.LtE())
        elif op_char == '>':
            ops.append(ast.Gt())
        elif op_char == '>=':
            ops.append(ast.GtE())
        else:  # !=
            ops.append(ast.NotEq)

        comparators.append(right)

    if len(ops) > 0:
        node = ast.Compare(left=node, ops=ops, comparators=comparators)

    return node


@set_lineno_col
def and_test_action(s, loc, tokens):
    if len(tokens) == 1:
        return tokens[0]
    else:
        return ast.BoolOp(op=ast.And(), values=tokens[:])


@set_lineno_col
def or_test_action(s, loc, tokens):
    if len(tokens) == 1:
        return tokens[0]
    else:
        return ast.BoolOp(op=ast.Or(), values=tokens[:])


@set_lineno_col
def test_action(s, loc, tokens):
    if len(tokens) == 1:
        return tokens[0]
    else:
        ast.IfExp(test=tokens[0], body=tokens[1], orelse=tokens[2])


@set_lineno_col
def expr_stmt_action(s, loc, tokens):
    node = tokens[0]

    if len(tokens) == 1:
        return ast.Expr(value=node)
    else:
        targets = []
        for t in tokens[:-1]:
            if isinstance(t, (ast.Name, ast.Attribute, ast.Subscript)):
                t.ctx = ast.Store()
                targets.append(t)
            else:
                raise RuntimeError('Cannot assign to expression')
        return ast.Assign(targets=targets, value=tokens[-1])


@set_lineno_col
def assert_stmt_action(s, loc, tokens):
    msg = tokens[1] if len(tokens) > 1 else None
    # TODO: wrap assert to raise custom BPCL Error?
    return ast.Assert(test=tokens[0], msg=msg)


@set_lineno_col
def print_stmt_action(s, loc, tokens):
    nl = tokens[-1] != ','
    values = [t for t in (tokens if nl else tokens[:-1])]
    if PY3:
        return ast.Call(
            func=ast.Name(id='print', ctx=ast.Load()),
            args=values, keywords=[], starargs=None, kwargs=None)
    else:
        return ast.Print(dest=None, values=values, nl=nl)


@set_lineno_col
def template_stmt_action(s, loc, tokens):
    node_list = []
    for token in tokens:
        node_list.append(
            ast.Expr(value=ast.Call(
                func=ast.Name(id='_template_{}'.format(token), ctx=ast.Load()),
                args=[], keywords=[], starargs=None, kwargs=None)
            )
        )
    return node_list


@set_lineno_col
def load_stmt_action(s, loc, tokens):
    node = ast.Expr(value=ast.Call(
        func=ast.Name(id='_load', ctx=ast.Load()),
        args=[tokens[0]], keywords=[], starargs=None, kwargs=None)
    )
    return node


@set_lineno_col
def if_stmt_action(s, loc, tokens):
    node = ast.If(test=tokens[0], body=tokens[1][:], orelse=[])
    curr_node = node
    for test, body in tokens.get('elif', ()):
        curr_node.orelse.append(ast.If(test=test, body=body[:], orelse=[]))
        curr_node = curr_node.orelse[-1]

    if 'else' in tokens:
        curr_node.orelse = tokens[-1][:]

    return node


@set_lineno_col
def for_stmt_action(s, loc, tokens):
    target = tokens[0]
    target.ctx = ast.Store()
    return ast.For(target=target, iter=tokens[1], body=tokens[2][:], orelse=[])


@set_lineno_col
def section_stmt_action(s, loc, tokens):
    body = tokens[1][:]

    if PY3:
        return ast.With(
            items=[ast.withitem(context_expr=ast.Call(
                func=ast.Name(id='_section', ctx=ast.Load()),
                args=[tokens[0]],
                keywords=[],
                starargs=None,
                kwargs=None
            ),
                optional_vars=None)],
            body=body
        )
    else:
        return ast.With(
            context_expr=ast.Call(
                func=ast.Name(id='_section', ctx=ast.Load()),
                args=[tokens[0]],
                keywords=[],
                starargs=None,
                kwargs=None
            ),
            optional_vars=None,
            body=body
        )


@set_lineno_col
def file_input_action(s, loc, tokens):
    return ast.Module(body=[t for t in tokens])


class BpclCompiler(object):
    def __init__(self):
        # Notes:
        #   ^ yields the longest match while | yield the first possible match

        # Enable Caching, otherwise it is super slow!!
        pp.ParserElement.enablePackrat()

        # Keywords
        k_load = pp.Keyword('load')
        k_section = pp.Keyword('section')
        k_template = pp.Keyword('template')
        k_assert = pp.Keyword('assert')
        k_print = pp.Keyword('print')
        k_if = pp.Keyword('if')
        k_else = pp.Keyword('else')
        k_elif = pp.Keyword('elif')
        k_for = pp.Keyword('for')
        k_in = pp.Keyword('in')
        k_break = pp.Keyword('break')
        k_continue = pp.Keyword('continue')

        keyword_list = [
            k_load, k_assert, k_print, k_template, k_section,
            k_if, k_else, k_elif, k_for, k_in, k_break, k_continue
        ]

        # Operators
        plus_op = pp.Literal('+')
        minus_op = pp.Literal('-')
        multiply_op = pp.Literal('*')
        divide_op = pp.Literal('/')
        modulus_op = pp.Literal('%')
        power_op = pp.Literal('**')
        dot_op = pp.Literal('.')
        colon_op = pp.Literal(':')
        assign_op = pp.Literal('=')
        comma_op = pp.Literal(',')
        lbracket_op = pp.Literal('[')
        rbracket_op = pp.Literal(']')
        lcurly_op = pp.Literal('{')
        rcurly_op = pp.Literal('}')
        lparen_op = pp.Literal('(')
        rparen_op = pp.Literal(')')
        qmark_op = pp.Literal('?')
        not_bit_op = pp.Literal('~')
        and_bit_op = pp.Literal('&')
        or_bit_op = pp.Literal('|')
        xor_bit_op = pp.Literal('^')
        lshift_bit_op, rshift_bit_op = pp.Literal('<<'), pp.Literal('>>')
        not_test_op = pp.Keyword('not')
        and_test_op = pp.Keyword('and')
        or_test_op = pp.Keyword('or')
        comp_op = (
            pp.Literal('==') ^ pp.Literal('>') ^ pp.Literal('>=') ^
            pp.Literal('<') ^ pp.Literal('<=') ^ pp.Literal('!=')
        )

        terminate_op = pp.LineEnd()

        # Number for integer, float and scientific notion
        number = pp.Combine(
            pp.Optional(pp.oneOf('+ -')) +
            (
                (pp.Word(pp.nums) + pp.Optional(dot_op + pp.Optional(pp.Word(pp.nums)))) |
                (dot_op + pp.Word(pp.nums))
            ) + pp.Optional(pp.oneOf('e E') + pp.Optional(pp.oneOf('+ -')) + pp.Word(pp.nums))
        ).setParseAction(number_action)

        dq_string = pp.QuotedString('"', escChar='\\', unquoteResults=False)
        sq_string = pp.QuotedString("'", escChar='\\', unquoteResults=False)
        q_string = (dq_string | sq_string)
        string = pp.Combine(
            pp.Optional(pp.oneOf('b u')) +
            q_string
        ).setParseAction(string_action)

        # Reserve the keywords and ensure they are NOT matched as a regular name
        # Variable names cannot start with underscore
        # This is avoid clashing with python internals (double underscore) and
        # the BPCL internals (single underscore)
        name = pp.Combine(
            ~pp.MatchFirst(keyword_list) + pp.Word(pp.alphas + '$', pp.alphanums + '_$')
        ).setParseAction(name_action)

        # Forward declarations
        argument = pp.Forward()
        factor = pp.Forward()
        not_test = pp.Forward()
        test = pp.Forward()
        stmt = pp.Forward()

        # Expressions
        # TODO: array literal?? may not be necessary
        atom = (
            number |
            string |
            name |
            (lparen_op.suppress() + test + rparen_op.suppress())
        ).setParseAction(passby_action)

        arglist = (
            pp.ZeroOrMore(argument + comma_op.suppress()) +
            argument +
            pp.Optional(comma_op).suppress()
        ).setParseAction(arglist_action)

        sliceop = (
            colon_op.suppress() + pp.Optional(test)
        ).setParseAction(sliceop_action)

        subscript = (
            test ^
            (
                pp.Optional(test)('start') + colon_op.suppress() +
                pp.Optional(test)('stop') +
                pp.Optional(sliceop)('step')
            )('slice')
        ).setParseAction(subscript_action)

        trailer = (
            (
                lparen_op.suppress() +
                pp.Optional(arglist) +
                rparen_op.suppress()
            )('func_call_trailer') |
            (
                lbracket_op.suppress() +
                subscript +
                rbracket_op.suppress()
            )('subscript_trailer') |
            (dot_op.suppress() + name)('attr_trailer')
        ).setParseAction(trailer_action)

        power = (
            atom +
            pp.ZeroOrMore(trailer)('trailers') +
            pp.Optional(power_op.suppress() + factor)('exponential')
        ).setParseAction(power_action)

        # Match power first so that negative value can be matched instead of unary op,
        # i.e. -1 is Num(n=-1) instead of UnaryOp(op=USub(), operand=Num(n=1))
        factor << (
            power |
            (
                (plus_op | minus_op | not_bit_op) + factor
            ).setParseAction(unary_action)
        ).setParseAction(factor_action)

        term = (
            factor +
            pp.ZeroOrMore(
                pp.Group((multiply_op | divide_op | modulus_op) + factor)
            )
        ).setParseAction(binop_action)

        arith_expr = (
            term +
            pp.ZeroOrMore(
                pp.Group((plus_op | minus_op) + term)
            )
        ).setParseAction(binop_action)

        shift_expr = (
            arith_expr +
            pp.ZeroOrMore(
                pp.Group((lshift_bit_op | rshift_bit_op) + arith_expr)
            )
        ).setParseAction(binop_action)

        and_expr = (
            shift_expr +
            pp.ZeroOrMore(
                pp.Group(and_bit_op + shift_expr)
            )
        ).setParseAction(binop_action)

        xor_expr = (
            and_expr +
            pp.ZeroOrMore(
                pp.Group(xor_bit_op + and_expr)
            )
        ).setParseAction(binop_action)

        expr = (
            xor_expr +
            pp.ZeroOrMore(
                pp.Group(or_bit_op + xor_expr)
            )
        ).setParseAction(binop_action)

        comparison = (
            expr +
            pp.ZeroOrMore(
                pp.Group(comp_op + expr)
            )
        ).setParseAction(compare_action)

        not_test << (
            (not_test_op + not_test).setParseAction(unary_action) |
            comparison
        ).setParseAction(passby_action)

        and_test = (
            not_test + pp.ZeroOrMore(and_test_op.suppress() + not_test)
        ).setParseAction(and_test_action)

        or_test = (
            and_test + pp.ZeroOrMore(or_test_op.suppress() + and_test)
        ).setParseAction(or_test_action)

        test << (
            or_test +
            pp.Optional(qmark_op.suppress() + or_test + colon_op.suppress() + test)
        ).setParseAction(test_action)

        kwarg = (
            name + assign_op.suppress() + test
        ).setParseAction(kwarg_action)

        argument << (
            test ^ kwarg
        ).setParseAction(passby_action)

        # Statements
        expr_stmt = (
            test +
            pp.ZeroOrMore(assign_op.suppress() + test)
        ).setParseAction(expr_stmt_action)

        assert_stmt = (
            k_assert.suppress() + test + pp.Optional(comma_op.suppress() + test)
        ).setParseAction(assert_stmt_action)

        print_stmt = (
            k_print.suppress() +
            pp.Optional(
                test + pp.ZeroOrMore(comma_op.suppress() + test) + pp.Optional(comma_op).suppress()
            )
        ).setParseAction(print_stmt_action)

        break_stmt = k_break.setParseAction(lambda tokens: ast.Break())
        continue_stmt = k_continue.setParseAction(lambda tokens: ast.Continue())

        template_verb = (pp.Literal('build') | pp.Literal('dump') |
                         pp.Literal('lint') | pp.Literal('compile') |
                         pp.Literal('recompile'))

        template_stmt = (
            k_template.suppress() + template_verb +
            pp.ZeroOrMore(comma_op.suppress() + template_verb) +
            pp.Optional(comma_op).suppress()
        ).setParseAction(template_stmt_action)

        load_stmt = (k_load.suppress() + string).setParseAction(load_stmt_action)

        simple_stmt = (
            assert_stmt | print_stmt | break_stmt | continue_stmt | expr_stmt |
            template_stmt | load_stmt
        ).setParseAction(passby_action)

        suite = (
            lcurly_op.suppress() +
            pp.ZeroOrMore(stmt) +
            rcurly_op.suppress()
        )

        if_stmt = (
            k_if.suppress() +
            test +
            pp.Group(suite) +
            pp.ZeroOrMore(
                pp.Group(k_elif.suppress() + test + pp.Group(suite))
            )('elif') +
            pp.Optional(k_else.suppress() + pp.Group(suite))('else')
        ).setParseAction(if_stmt_action)

        for_stmt = (
            k_for.suppress() +
            expr +
            k_in.suppress() +
            test +
            pp.Group(suite)
        ).setParseAction(for_stmt_action)

        section_stmt = (
            k_section.suppress() +
            number +
            pp.Group(suite)
        ).setParseAction(section_stmt_action)

        compound_stmt = (if_stmt | for_stmt | section_stmt)

        # TODO: This allows multiple stmts to be written on the same line with only whitespaces
        #       as separators. Tried to enforce newline as separator between stmts and did NOT work.
        stmt << (compound_stmt | simple_stmt | terminate_op.suppress())

        self.file_input = pp.ZeroOrMore(stmt).ignore(
            pp.pythonStyleComment).setParseAction(file_input_action)

    def parse(self, text):
        """
        Parse the given text with file_input parser
        :param text:
        :return:
        :rtype: pp.ParsedResults
        """
        try:
            parsed_results = self.file_input.parseString(text, parseAll=True)
        except pp.ParseException as e:
            print(e.line)
            print('{}^'.format(' ' * (e.col - 1)))
            raise

        tree = parsed_results[0]
        ast.fix_missing_locations(tree)
        return tree

    def compile(self, inp):
        if isinstance(inp, str):
            inp = self.parse(inp)
        code_object = compile(inp, '<string>', mode='exec')
        return code_object


bpcl_compiler = BpclCompiler()
