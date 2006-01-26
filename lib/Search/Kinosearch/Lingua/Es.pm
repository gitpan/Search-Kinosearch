package Search::Kinosearch::Lingua::Es;
use strict;
use warnings;

use Lingua::Stem::Snowball qw();

our $tokenreg = qr/\w+/;

my $stemmer = Lingua::Stem::Snowball->new(-language => 'spanish');

sub stem {
    my $either = shift; 
    
    my @stemmed = $stemmer->stem(\@_);
    
    return \@stemmed;
}

##############################################################################
### Spanish tokenizer is a wrapper for &Search::Kinosearch::Lingua::tokenize
##############################################################################
sub tokenize {
    return &Search::Kinosearch::Lingua::tokenize(@_);
}

our $stoplist = {
	actual => undef,
	alguna => undef,
	algunas => undef,
	alguno => undef,
	algunos => undef,
	algún => undef,
	ambos => undef,
	ampleamos => undef,
	ante => undef,
	antes => undef,
	aquel => undef,
	aquellas => undef,
	aquellos => undef,
	aqui => undef,
	arriba => undef,
	atras => undef,
	bajo => undef,
	bastante => undef,
	bien => undef,
	cada => undef,
	cierta => undef,
	ciertas => undef,
	cierto => undef,
	ciertos => undef,
	como => undef,
	con => undef,
	conseguimos => undef,
	conseguir => undef,
	consigo => undef,
	consigue => undef,
	consiguen => undef,
	consigues => undef,
	cual => undef,
	cuando => undef,
	de => undef,
	dentro => undef,
	desde => undef,
	donde => undef,
	dos => undef,
	el => undef,
	ellas => undef,
	ellos => undef,
	empleais => undef,
	emplean => undef,
	emplear => undef,
	empleas => undef,
	empleo => undef,
	en => undef,
	encima => undef,
	entonces => undef,
	entre => undef,
	era => undef,
	eramos => undef,
	eran => undef,
	eras => undef,
	eres => undef,
	es => undef,
	esta => undef,
	estaba => undef,
	estado => undef,
	estais => undef,
	estamos => undef,
	estan => undef,
	estoy => undef,
	fin => undef,
	fue => undef,
	fueron => undef,
	fui => undef,
	fuimos => undef,
	gueno => undef,
	ha => undef,
	hace => undef,
	haceis => undef,
	hacemos => undef,
	hacen => undef,
	hacer => undef,
	haces => undef,
	hago => undef,
	incluso => undef,
	intenta => undef,
	intentais => undef,
	intentamos => undef,
	intentan => undef,
	intentar => undef,
	intentas => undef,
	intento => undef,
	ir => undef,
	la => undef,
	largo => undef,
	las => undef,
	lo => undef,
	los => undef,
	mientras => undef,
	mio => undef,
	modo => undef,
	muchos => undef,
	muy => undef,
	nos => undef,
	nosotros => undef,
	otro => undef,
	para => undef,
	pero => undef,
	podeis => undef,
	podemos => undef,
	poder => undef,
	podria => undef,
	podriais => undef,
	podriamos => undef,
	podrian => undef,
	podrias => undef,
	por => undef,
	'por qué' => undef,
	porque => undef,
	primero => undef,
	puede => undef,
	pueden => undef,
	puedo => undef,
	quien => undef,
	sabe => undef,
	sabeis => undef,
	sabemos => undef,
	saben => undef,
	saber => undef,
	sabes => undef,
	se => undef,
	ser => undef,
	si => undef,
	siendo => undef,
	'sin' => undef,
	sobre => undef,
	sois => undef,
	solamente => undef,
	solo => undef,
	somos => undef,
	soy => undef,
	su => undef,
	sus => undef,
	también => undef,
	teneis => undef,
	tenemos => undef,
	tener => undef,
	tengo => undef,
	tiempo => undef,
	tiene => undef,
	tienen => undef,
	todo => undef,
	trabaja => undef,
	trabajais => undef,
	trabajamos => undef,
	trabajan => undef,
	trabajar => undef,
	trabajas => undef,
	trabajo => undef,
	tras => undef,
	tuyo => undef,
	ultimo => undef,
	un => undef,
	una => undef,
	unas => undef,
	uno => undef,
	unos => undef,
	usa => undef,
	usais => undef,
	usamos => undef,
	usan => undef,
	usar => undef,
	usas => undef,
	uso => undef,
	va => undef,
	vais => undef,
	valor => undef,
	vamos => undef,
	van => undef,
	vaya => undef,
	verdad => undef,
	verdadera => undef,
	verdadero => undef,
	vosotras => undef,
	vosotros => undef,
	voy => undef,
	y => undef,
	yo => undef,
    };

1;

__END__

=head1 NAME

Search::Kinosearch::Lingua::Es - Kinosearch Spanish language functions

=head1 DEPRECATED

Search::Kinosearch has been superseded by L<KinoSearch|KinoSearch>.  Please
use the new version.

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
