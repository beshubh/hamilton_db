//! The dialect statements are parsed in: PostgreSQL's, with a quicker
//! path to literals.

use std::any::TypeId;

use sqlparser::ast::Expr;
use sqlparser::dialect::{Dialect, PostgreSqlDialect, Precedence};
use sqlparser::keywords::Keyword;
use sqlparser::parser::{Parser, ParserError};
use sqlparser::tokenizer::Token;

/// [`PostgreSqlDialect`], with every method it overrides forwarded to it,
/// and its type id as its own, so that sqlparser parses exactly as it does
/// for PostgreSQL. An expression that starts with a number or a quoted
/// string is that literal, but sqlparser first tries to read a type name
/// there, for typed literals such as `DATE '...'`, and formats an error
/// message each time it fails. Taking the literal straight away gives the
/// same expression without that, which adds up in an INSERT of many rows.
#[derive(Debug, Default)]
pub(crate) struct HamiltonDialect(PostgreSqlDialect);

impl Dialect for HamiltonDialect {
    fn dialect(&self) -> TypeId {
        self.0.dialect()
    }

    fn parse_prefix(&self, parser: &mut Parser) -> Option<Result<Expr, ParserError>> {
        match parser.peek_token_ref().token {
            Token::Number(..) | Token::SingleQuotedString(_) => {
                Some(parser.parse_value().map(Expr::Value))
            }
            _ => None,
        }
    }

    fn identifier_quote_style(&self, _identifier: &str) -> Option<char> {
        self.0.identifier_quote_style(_identifier)
    }

    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        self.0.is_delimited_identifier_start(ch)
    }

    fn is_identifier_start(&self, ch: char) -> bool {
        self.0.is_identifier_start(ch)
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        self.0.is_identifier_part(ch)
    }

    fn supports_unicode_string_literal(&self) -> bool {
        self.0.supports_unicode_string_literal()
    }

    fn is_reserved_for_identifier(&self, kw: Keyword) -> bool {
        self.0.is_reserved_for_identifier(kw)
    }

    fn is_table_alias(&self, kw: &Keyword, _parser: &mut Parser) -> bool {
        self.0.is_table_alias(kw, _parser)
    }

    fn is_custom_operator_part(&self, ch: char) -> bool {
        self.0.is_custom_operator_part(ch)
    }

    fn get_next_precedence(&self, parser: &Parser) -> Option<Result<u8, ParserError>> {
        self.0.get_next_precedence(parser)
    }

    fn supports_filter_during_aggregation(&self) -> bool {
        self.0.supports_filter_during_aggregation()
    }

    fn supports_group_by_expr(&self) -> bool {
        self.0.supports_group_by_expr()
    }

    fn supports_alter_user_as_alter_role(&self) -> bool {
        self.0.supports_alter_user_as_alter_role()
    }

    fn prec_value(&self, prec: Precedence) -> u8 {
        self.0.prec_value(prec)
    }

    fn allow_extract_custom(&self) -> bool {
        self.0.allow_extract_custom()
    }

    fn allow_extract_single_quotes(&self) -> bool {
        self.0.allow_extract_single_quotes()
    }

    fn supports_create_index_with_clause(&self) -> bool {
        self.0.supports_create_index_with_clause()
    }

    fn supports_explain_with_utility_options(&self) -> bool {
        self.0.supports_explain_with_utility_options()
    }

    fn supports_listen_notify(&self) -> bool {
        self.0.supports_listen_notify()
    }

    fn supports_exclude_constraint(&self) -> bool {
        self.0.supports_exclude_constraint()
    }

    fn supports_factorial_operator(&self) -> bool {
        self.0.supports_factorial_operator()
    }

    fn supports_bitwise_shift_operators(&self) -> bool {
        self.0.supports_bitwise_shift_operators()
    }

    fn supports_comment_on(&self) -> bool {
        self.0.supports_comment_on()
    }

    fn supports_load_extension(&self) -> bool {
        self.0.supports_load_extension()
    }

    fn supports_named_fn_args_with_colon_operator(&self) -> bool {
        self.0.supports_named_fn_args_with_colon_operator()
    }

    fn supports_named_fn_args_with_expr_name(&self) -> bool {
        self.0.supports_named_fn_args_with_expr_name()
    }

    fn supports_empty_projections(&self) -> bool {
        self.0.supports_empty_projections()
    }

    fn supports_nested_comments(&self) -> bool {
        self.0.supports_nested_comments()
    }

    fn supports_string_escape_constant(&self) -> bool {
        self.0.supports_string_escape_constant()
    }

    fn supports_numeric_literal_underscores(&self) -> bool {
        self.0.supports_numeric_literal_underscores()
    }

    fn supports_array_typedef_with_brackets(&self) -> bool {
        self.0.supports_array_typedef_with_brackets()
    }

    fn supports_geometric_types(&self) -> bool {
        self.0.supports_geometric_types()
    }

    fn supports_order_by_using_operator(&self) -> bool {
        self.0.supports_order_by_using_operator()
    }

    fn supports_set_names(&self) -> bool {
        self.0.supports_set_names()
    }

    fn supports_alter_column_type_using(&self) -> bool {
        self.0.supports_alter_column_type_using()
    }

    fn supports_left_associative_joins_without_parens(&self) -> bool {
        self.0.supports_left_associative_joins_without_parens()
    }

    fn supports_notnull_operator(&self) -> bool {
        self.0.supports_notnull_operator()
    }

    fn supports_interval_options(&self) -> bool {
        self.0.supports_interval_options()
    }

    fn supports_insert_table_alias(&self) -> bool {
        self.0.supports_insert_table_alias()
    }

    fn supports_create_table_like_parenthesized(&self) -> bool {
        self.0.supports_create_table_like_parenthesized()
    }

    fn supports_select_wildcard_with_alias(&self) -> bool {
        self.0.supports_select_wildcard_with_alias()
    }

    fn supports_comma_separated_trim(&self) -> bool {
        self.0.supports_comma_separated_trim()
    }

    fn supports_xml_expressions(&self) -> bool {
        self.0.supports_xml_expressions()
    }

    fn supports_aliased_function_args(&self) -> bool {
        self.0.supports_aliased_function_args()
    }

    fn supports_comment_optimizer_hint(&self) -> bool {
        self.0.supports_comment_optimizer_hint()
    }
}
