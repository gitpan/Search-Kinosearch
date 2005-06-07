package Search::Kinosearch::Lingua::Hr;
use strict;
use warnings;

### DEVNOTE: This module is completely untested.

use utf8;

use Lingua::Spelling::Alternative qw();

### Matches one token in Croatian.
our $tokenreg = qr/\w+/;

my $alt_speller = Lingua::Spelling::Alternative->new();

sub _init {
    my $either = shift;
    my $ispell_affix_file = shift;
    croak("Can't access ispell affix file '$ispell_affix_file': $!") 
        unless -e $ispell_affix_file;
    $alt_speller->load_affix($ispell_affix_file);
}

##############################################################################
### Croatian stemmer
##############################################################################
sub stem {
    my $either = shift;
    
    my @quasi_normalized = $alt_speller->minimal(@_);
    
    for my $i (0 .. $#quasi_normalized) {
        if ($quasi_normalized[$i] eq '') {
            $quasi_normalized[$i] = $_[$i];
        }
    }

    return \@quasi_normalized;
}

##############################################################################
### Croatian tokenizer is a wrapper for &Search::Kinosearch::Lingua::tokenize
##############################################################################
sub tokenize {
    return &Search::Kinosearch::Lingua::tokenize(@_);
}

our $stoplist = {};

1;


__END__

=head1 NAME

Search::Kinosearch::Lingua::Hr - Kinosearch Croatian language functions

=head1 WARNING

Croatian language functions are currently unsupported in Kinosearch.

=head1 SYNOPSIS

No public interface.

=head1 DESCRIPTION

This subclass implementation of Search::Kinosearch::Lingua is a helper 
class for Search::Kinosearch.  Do not use it by itself.  

=begin comment

stem() and tokenize() are private.

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

=back

=head1 AUTHOR

Marvin Humphrey E<lt>marvin at rectangular dot comE<gt>
L<http://www.rectangular.com>

=head1 COPYRIGHT

Copyright (c) 2005 Marvin Humphrey.  All rights reserved.
This module is free software.  It may be used, redistributed and/or 
modified under the same terms as Perl itself.

=cut

