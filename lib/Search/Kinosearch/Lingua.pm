package Search::Kinosearch::Lingua;
use strict;
use warnings;

use Carp;

use Search::Kinosearch;

### Regex for a single token.
our $tokenreg = qr/\b\w+\b/;

##############################################################################
### Search::Kinosearch::Lingua is an abstract base class.
##############################################################################
sub new {
    &Search::Kinosearch::abstract_death (__PACKAGE__, 'new()');
}

##############################################################################
### Default tokenizing routine - a blunt instrument.
##############################################################################
sub tokenize {
    my $self = shift;
    my $input = shift;
    
    return ([], "\0\0") unless (length $input);

    my $token_regex = $self->{tokenreg};
    confess("Undefined tokenreg") unless defined $token_regex;

    for ($input) {
        my @tokenized;
        my @positions;
        my $pos = 0;
        while ($input =~ s/(.*?)($token_regex)//) {
            push @tokenized, $2;
            use bytes;
            $pos += length $1;
            push @positions, $pos;
            $pos += length($2);
        }
        
        return (\@tokenized, \@positions);
    }
}

##############################################################################
### The default stem routine returns a duplicate of the input.
##############################################################################
sub stem {
    my $self = shift;
    my $input = shift;
    my @no_change = @$input;
    return \@no_change;
}

1;

__END__

=head1 NAME

Search::Kinosearch::Lingua - Language-specific Kinosearch functions

=head1 SYNOPSIS

    ### Search::Kinosearch::Lingua is an abstract base class.
    ### Search::Kinosearch::Lingua::Xx subclasses are invoked indirectly.

    ### Example 1: A Spanish Kindexer object
    my $kindexer = Search::Kinosearch::Kindexer->new(
        -language => 'Es',
        );

    ### Example 2: An English KSearch object (language of 'En' is implicit)
    my $ksearch = Search::Kinosearch::KSearch->new()

=head1 DESCRIPTION

The purpose of the Search::Kinosearch::Lingua::Xx subclasses is to provide
language-specific functionality to the rest of the Kinosearch suite.  Code is
loaded indirectly, based on the -language parameter for either
Search::Kinosearch::Kindexer->new() or
Search::Kinosearch::KSearch->new().  

All Search::Kinosearch::Lingua::Xx subclasses implement two methods:
tokenize and stem.  The code in these methods is reused by the
following:

=over

=item

&Search::Kinosearch::Kindexer::tokenize_field

=item

&Search::Kinosearch::Kindexer::stem_field

=item

&Search::Kinosearch::KSearch::process

=back

Additionally, each Lingua::Xx subclass contains a default stoplist and a
precompiled regex matching a single token.

Kindexer and KSearch default to 'En' (English); however, it is possible to
specify no language: -language => '', in which case the default tokenize() and
stem() methods from the base class Search::Kinosearch::Lingua will be
utilized.

=begin comment

new() is private.

=head1 CONSTRUCTOR

=head2 new()

=end comment

=head1 METHODS

=head2 tokenize()

Tokenizing is the process of breaking up a stream of symbols into pieces.  The
default tokenize() routine, invoked when a language of '' [empty string] is
specified, is quite crude -- basically, all it does is split on whitespace.
(For comparison, the default English-language tokenizer converts most non-word
characters to spaces (apostrophes receive special treatment) prior to
splitting on whitespace.)  

=head2 stem() 

stem() provides a wrapper for a language-specific stemming algorithm.  For a
conceptual explanation of stemming, see the documentation for
L<Lingua::Stem|Lingua::Stem>. 

Currently, the L<Lingua::Stem::Snowball|Lingua::Stem::Snowball> stemmers are
preferred for performance reasons.  

=begin comment

### DEVNOTE: We won't address alternative stemming possibilities ntil the 
### core of Kinosearch is stabilized.

However, Snowball stemmers only exist for
a limited number of languages, so for others it is necessary to implement the
stem() algorithm some other way.  In cases where no stemming algorithm is
available, e.g.  Croatian, Kinosearch provides an approximation of stemming
functionality via the minimal() method of
L<Lingua::Spelling::Alternative.|Lingua::Spelling::Alternative.> Should a
Snowball stemming algorithm become available for Croatian, it is likely that
the implementation in
L<Search::Kinosearch::Lingua::Hr|Search::Kinosearch::Lingua::Hr> will change.

=end comment

=head1 TODO

=over

=item

Implement Search::Kinosearch::Lingua::Xx modules for as many languages as
possible.

=item

Consider enabling alternative stemming routines for languages with no Snowball
stemmer.

=back

=head1 SEE ALSO

=over

=item

L<Search::Kinosearch|Search::Kinosearch>

=item

L<Search::Kinosearch::Kindexer|Search::Kinosearch::Kindexer>

=item

L<Search::Kinosearch::KSearch|Search::Kinosearch::KSearch>

=item

L<Search::Kinosearch::Tutorial|Search::Kinosearch::Tutorial>

=back

=head1 AUTHOR

Marvin Humphrey E<lt>marvin at rectangular dot comE<gt> 
L<http://www.rectangular.com>

=head1 COPYRIGHT

Copyright (c) 2005 Marvin Humphrey.  All rights reserved.
This module is free software.  It may be used, redistributed and/or 
modified under the same terms as Perl itself.

=cut

