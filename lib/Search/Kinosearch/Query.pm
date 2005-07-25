package Search::Kinosearch::Query;
use strict;
use warnings;

use attributes 'reftype';

use Carp;

##############################################################################
### Constructor
##############################################################################
sub new {
    my $class = shift;
    my $self = bless {}, ref($class) || $class;
    $self->_init_query(@_);
    return $self;
}

my %init_query_defaults = (
    -string         => undef,
    -any_or_all     => 'any',
    -lowercase      => 0,
    -tokenize       => 0,
    -stem           => 0,
    -required       => 0,
    -negated        => 0,
    -fields         => undef,
    -allow_boolean  => 1,
    -allow_phrases  => 1,
    -min_sortstring => undef,
    -max_sortstring => undef,
    -min_date       => undef, 
    -max_date       => undef,
    productions     => undef,
    set_size        => 0,
    prodtype        => 'query',
);

##############################################################################
### Initialize a Query object.
##############################################################################
sub _init_query {
    my $self = shift;
    
    ### Verify and assign parameters.
    %$self = %init_query_defaults;
    while (@_) {
        my ($var, $val)  = (shift, shift);
        croak ("invalid parameter: '$var'" )
            unless exists $init_query_defaults{$var};
        $self->{$var} = $val;
    }

    ### The original search string is preserved in {-string}.  
    ### Manipulations will use {qstring}.
    $self->{qstring}  = $self->{-string};
    $self->{required} = $self->{-required};
    $self->{negated}  = $self->{-negated};
    
}

package Search::Kinosearch::Query::Compound;

use base qw( Search::Kinosearch::Query );

package Search::Kinosearch::Query::Term;

use base qw( Search::Kinosearch::Query );

package Search::Kinosearch::Query::Phrase;

use base qw( Search::Kinosearch::Query );

package Search::Kinosearch::Query::BoolGroup;

use base qw( Search::Kinosearch::Query );

package Search::Kinosearch::Query::BoolOp;

use base qw( Search::Kinosearch::Query );


1;


__END__

=head1 NAME

Search::Kinosearch::Query - create queries to feed to KSearch 

=head1 SYNOPSIS

    my $query = Search::Kinosearch::Query->new(
        -string     => 'this AND NOT (that OR "the other thing")',
        -lowercase  => 1,
        -tokenize   => 1,
        -stem       => 1,
        );
    $ksearch->add_query( $query );
    $ksearch->process;
    
    while (my $results = $ksearch->fetch_result_hashref) {
        print "$results->{title}\n";
    }

=head1 DESCRIPTION

=head2 Query syntax

Operators, in descending order of precedence:

=over 

=item "double quotes"

If phrases are enabled, then passages surrounded by double quotes are
evaluated as phrases.  If the closing quote is omitted, the phrase is defined
as closed by the end of the query string.  No other operators are evaluated
within a phrase.

=item prepended +plus and -minus

Require or negate an item.  It is possible to require or negate a phrase: 
'foo -"bar baz"' ... or a parenthetical group: 'foo +(bar baz)'.  

=item (parentheses)

Bind items into logical groups.

=item x AND y, x OR y, x AND NOT y

These three operators, whose primary functions should be obvious,  have the
effect of grouping the items on either side of them, as if surrounded by
parentheses.

Note that Kinosearch's "boolean" queries do not support a full complement of
boolean operators.  For XOR and NAND, that's because so few people would ever
use them.  More subtly, NOT is only available as half of AND NOT, 
because in the vast majority of cases it would undesirable for the query
"NOT freekezoidzzzzz" to return every document in the kindex.  Prepended
-minuses are treated the same way: '-foo' on its own returns no documents.  In
both cases the negated set must be subtracted from something in order to have
any effect: 'foo AND NOT bar', 'foo -bar'.  Isolating a term with a prepended
-minus inside parentheses turns it into a no-op: 'foo (-bar)' returns all docs
containing 'foo', including those that contain 'bar'.

=back

=head1 CONSTRUCTOR

=head2 new()

    my $query = Search::Kinosearch::Query->new(
        -string     => $query_string,   # Required.
        -lowercase  => 1,               # Default: 0
        -tokenize   => 1,               # Default: 0
        -stem       => 1,               # Default: 0
        -required   => 1,               # Default: 0
        -negated    => 0,               # Default: 0
        -fields     => {                # Default: use aggregate score
                           title    => 3,
                           bodytext => 1, 
                       },
        );

Add a query to the KSearch object.  

=over

=item -string

The string to be matched against.

=item -lowercase

Convert query to lower case.

=item -tokenize

Tokenize the query.  

=item -stem

Stem terms within the query.

=item -required

Return only results which match this query.

=item -negated

Return only results which do NOT match this query.

=item -fields

Specify fields to search against in the kindex.  Must be supplied as a hash,
with the fields to be searched as keys and the weight that those fields are to
be given as values.

=back

=head1 TO DO

=over

=item

Consider whether it might be better to remove parameters such as
-allow_boolean from KSearch, instead creating subclasses like
Search::Kinosearch::Query::Boolean.

=item

Implement subclasses.

=back

=head1 SEE ALSO

=over

=item

L<Search::Kinosearch::KSearch|Search::Kinosearch::KSearch>

=back

=head1 AUTHOR

Marvin Humphrey <marvin at rectangular dot com>

=head1 COPYRIGHT

Copyright (c) 2005 Marvin Humphrey.  All rights reserved.
This module is free software.  It may be used, redistributed and/or 
modified under the same terms as Perl itself.

=cut
