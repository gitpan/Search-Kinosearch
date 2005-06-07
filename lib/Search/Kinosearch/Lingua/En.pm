package Search::Kinosearch::Lingua::En;
use strict;
use warnings;

use base qw( Search::Kinosearch::Lingua );
use Lingua::Stem::Snowball qw();

my $stemmer = Lingua::Stem::Snowball->new(lang => 'en');

### Matches one token in English.
our $tokenreg = qr/\b\w+(?:'\w+)?\b/;

##############################################################################
### English stemmer
##############################################################################
sub stem {
    my $self = shift;
    my $to_stem = shift;

    return [] unless @$to_stem;
    
    my @stemmed;
    
    ### The Snowball stemmer chokes if the first item in the input array 
    ### is either undefined or whitespace.
    while ($to_stem->[0] eq '' and @$to_stem) {
        push @stemmed, shift @$to_stem;
    }
    
    ### The Snowball stemmer misbehaves unless you strip "'s" beforehand.
    for (@$to_stem) {
        s/'s$//;
    }
    
    @stemmed = (@stemmed, $stemmer->stem($to_stem));
    return \@stemmed;
}

##############################################################################
### English tokenizer is a wrapper for &Search::Kinosearch::Lingua::tokenize
##############################################################################
sub tokenize {
    return &Search::Kinosearch::Lingua::tokenize(@_);
}

our $stoplist = {
    'a' => undef,
    'about' => undef,
    'also' => undef,
    'an' => undef,
    'and' => undef,
    'another' => undef,
    'any' => undef,
    'are' => undef,
    'as' => undef,
    'at' => undef,
    'back' => undef,
    'be' => undef,
    'because' => undef,
    'been' => undef,
    'being' => undef,
    'but' => undef,
    'by' => undef,
    'can' => undef,
    'could' => undef,
    'did' => undef,
    'do' => undef,
    'each' => undef,
    'end' => undef,
    'even' => undef,
    'for' => undef,
    'from' => undef,
    'get' => undef,
    'go' => undef,
    'had' => undef,
    'have' => undef,
    'he' => undef,
    'her' => undef,
    'here' => undef,
    'his' => undef,
    'how' => undef,
    'i' => undef,
    'if' => undef,
    'in' => undef,
    'into' => undef,
    'is' => undef,
    'it' => undef,
    'just' => undef,
    'may' => undef,
    'me' => undef,
    'might' => undef,
    'much' => undef,
    'must' => undef,
    'my' => undef,
    'no' => undef,
    'not' => undef,
    'of' => undef,
    'off' => undef,
    'on' => undef,
    'only' => undef,
    'or' => undef,
    'other' => undef,
    'our' => undef,
    'out' => undef,
    'should' => undef,
    'so' => undef,
    'some' => undef,
    'still' => undef,
    'such' => undef,
    'than' => undef,
    'that' => undef,
    'the' => undef,
    'their' => undef,
    'them' => undef,
    'then' => undef,
    'there' => undef,
    'these' => undef,
    'they' => undef,
    'this' => undef,
    'those' => undef,
    'to' => undef,
    'too' => undef,
    'try' => undef,
    'two' => undef,
    'under' => undef,
    'up' => undef,
    'us' => undef,
    'was' => undef,
    'we' => undef,
    'were' => undef,
    'what' => undef,
    'when' => undef,
    'where' => undef,
    'which' => undef,
    'while' => undef,
    'who' => undef,
    'why' => undef,
    'will' => undef,
    'with' => undef,
    'within' => undef,
    'without' => undef,
    'would' => undef,
    'you' => undef,
    'your' => undef,
    };

1;


__END__

=head1 NAME

Search::Kinosearch::Lingua::En - Kinosearch English-language functions

=head1 SYNOPSIS

No public interface.

=head1 DESCRIPTION

This subclass implementation of Search::Kinosearch::Lingua is a helper 
class for Search::Kinosearch.  Do not use it by itself.  

=begin comment

Private subroutines:

=head1 METHODS

=head2 stem()

=head2 tokenize()

=end comment

=head1 SEE ALSO

=over

=item

L<Search::Kinosearch::Lingua|Search::Kinosearch::Lingua>

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
