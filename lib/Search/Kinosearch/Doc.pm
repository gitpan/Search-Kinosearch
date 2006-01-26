package Search::Kinosearch::Doc;
use strict;
use warnings;

use Carp;

### This is just for reference.  Both Kindexer and KSearch need to create
### Doc objects very very often, so to speed things up a little, they pass 
### a hashref to the constructor to be used as $self.
# my %new_defaults = (
#   fields => undef,
#   sortstring => undef,
#   sortstring_bytes => 0,
#   datetime_string => undef,
#   datetime_ymdhms => undef,
# );
##############################################################################
### Constructor
##############################################################################
sub new {
    my $class = shift;
    my $self = shift || {};
    bless $self, $class;
}

sub set_field {
    my $self = shift;
    croak("Expecting a name => value pair") unless @_ == 2;
    my ($fieldname, $val) = (@_);
    defined $val or $val = '';
    ### Collapse whitespace.
    ### This shouldn't be necessary, but for some reason the positional data
    ### gets off if we don't.  TODO: debug.
    $val =~ s/\s+/ /g;
    $val =~ s/^\s//g;
    $val =~ s/\s$//g;
    $self->{fields}{$fieldname} = $val;
}

sub get_field {
    my $self = shift;
    my $fieldname = shift;
    return $self->{fields}{$fieldname};
}

##############################################################################
### Set the datetime for this document.
##############################################################################
sub set_datetime {
    my $self = shift;
    my @args = @_;
    croak("Expecting 6 arguments, but got " . scalar(@_) . ": @args")
        unless @_ == 6;
    $self->{datetime_ymdhms} = \@args; 
    $self->{datetime_string} = pack("c n c c c c c ", 0,@args);
}

##############################################################################
### Set the sortstring for this document. EXPERIMENTAL!
##############################################################################
sub set_sortstring {
    my $self = shift;
    $self->{sortstring} = shift;
}

1;
    
__END__

=head1 NAME

Search::Kinosearch::Doc - a document.

=head1 DEPRECATED

Search::Kinosearch has been superseded by L<KinoSearch|KinoSearch>.  Please
use the new version.

=head1 SYNOPSIS

    my $kindexer = Search::Kinosearch::Kindexer->new;
    ...
    my $doc = $kindexer->new_doc( $unique_doc_id );
    
    ### or...
    
    my $ksearch = Search::Kinosearch::KSearch->new;
    ...
    while (my $doc = $ksearch->fetch_next_result) {
        print $doc->get_field('title') . "\n";
    }

=head1 DESCRIPTION

Search::Kinosearch::Doc objects are logical representations of documents that
you feed into the Kindexer.  They are organized as collections of fields --
like database rows.

=head1 CONSTRUCTOR

=begin comment

=head2 new()

=end comment

Not accessible.  Doc objects must be spawned by either Kindexer or KSearch
objects.

=head1 METHODS

=head2 set_field()

Set the contents of a field.

=head2 get_field()

Retrieve the contents of a field.

=head2 set_datetime()

    $doc->set_datetime($Y,$M,$D,$h,$m,$s);

Assign a datetime to the document.  6 arguments are required: year month day
hour minute second.  Kinosearch uses the same conventions as Steffen Beyer's
excellent L<Date::Calc|Date::Calc> module, which you will probably want to
use.  

The value for the year must fit within a 16 bit signed int (at least until
someone points me at a module that can turn YMDHMS into a 64bit timestamp).

=begin comment

=head2 set_sortstring()

Experimental.

=end comment

=head1 SEE ALSO

=over

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


