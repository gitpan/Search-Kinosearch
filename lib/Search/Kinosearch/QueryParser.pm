package Search::Kinosearch::QueryParser;
use strict;
use warnings;

use base qw( Search::Kinosearch );
use attributes 'reftype';

use Carp;
use Storable qw( nfreeze thaw );

my $phrasereg = qr/"([^"]*[^"\s]+[^"]*)(?:"|$)/;
my $parenreg  = qr/\(([^()]+[^()\s]*[^()]*)(?:\)|$)/;
my $boolopreg = qr/\b(?:AND NOT|AND|OR)\b/;
my $unopreg   = qr/(?:^|(?<=\s))(?:\+|-)\b/;

##############################################################################
### Constructor
##############################################################################
sub new {
    my $class = shift;
    my $self = bless {}, ref($class) || $class;
    $self->_init_queryparser(@_);
    return $self;
}

my %init_queryparser_defaults = (
    -max_terms         => undef,
    -stoplist          => undef,
    -language          => undef,
);

##############################################################################
### Initialize a QueryParser object.
##############################################################################
sub _init_queryparser {
    my $self = shift;
    %$self = (%init_queryparser_defaults, %$self);

    while (@_) {
        my $var = shift;
        my $val = shift;
        croak("Invalid parameter: $var")
            unless exists $init_queryparser_defaults{$var};
        $self->{$var} = $val;
    }

    my @chars = ('A' .. 'Z');
    $self->{randstring} .= $chars[rand @chars] for (1 .. 8);
    my $randstring = $self->{randstring};
    
    $self->{labelreg}  = qr/__(?:phrase|paren|booolgroup)$randstring\d{4}/;

    $self->{productions} = [];
    $self->{paren_storage}  = {};
    $self->{phrase_storage} = {};
    $self->{paren_inc}      = 0;
    $self->{phrase_inc}     = 0;
    $self->{searchterms}    = [];
        
    ### Define tokenizing, stemming, $tokenreg, stoplist, etc.
    $self->define_language_specific_functions;
}

#my %parse_defaults = (
#    -string         => undef,
#    -any_or_all     => 'any',
#    -lowercase      => 0,
#    -tokenize       => 0,
#    -stem           => 0,
#    -required       => 0,
#    -negated        => 0,
#    -fields         => undef,
#    -allow_boolean  => 1,
#    -allow_phrases  => 1,
#    -min_sortstring => undef,
#    -max_sortstring => undef,
#    -min_date       => undef, 
#    -max_date       => undef,
#    productions     => undef,
#    set_size        => 0,
#    prodtype        => 'query',
#);

##############################################################################
### Parse a query.
##############################################################################
sub parse {
    my $self = shift;
    my @queries = @_;
    push @{ $self->{productions} }, @queries;
    return unless @{ $self->{productions} };
    
#   if (my $max = $self->{-max_terms}) {
#        if($query->{qstring} =~ s/
#                (
#                    (?:(?:AND|OR|NOT)\s+)*    # Any number of boolean qualifiers
#                    (?:\S+\s*)
#                ){0,$max}                    # Up to $max number of terms;
#                (.*)                        # Capture any extra terms
#                /$1/x) {
#            my $extra = $2;
#            if ($extra) {
#                push @{ $self->{warnings} }, "$extra";
#            }
#        }
#    }

    $self->_replace_phrases($_, $_) for @{ $self->{productions} };


    do {
        $self->_grow_branches($_, $_) for @{ $self->{productions} };
        for (@{ $self->{productions} }) {
            $self->_replace_parens($_, $_)
                if $_->{-tokenize};            
            $self->_define_and_tokenize($_, $_); 
            $self->_isolate_boolgroups($_, $_)
                if $_->{-tokenize};
        }
    } while %{ $self->{paren_storage} };
            
    $self->_tokenize_for_real($_, $_)           for @{ $self->{productions} };
    $self->_apply_boolops($_, $_)               for @{ $self->{productions} };
    $self->_derive_required_and_negated($_, $_) for @{ $self->{productions} };
    $self->_apply_case_folding($_, $_)          for @{ $self->{productions} };
    $self->_apply_stemming($_, $_)              for @{ $self->{productions} };
    $self->_expand_phrases($_, $_)              for @{ $self->{productions} };

    return $self->{productions};
}

#############################################################################
### Logical groups, indicated by paretheses in English, e.g.
### "foo (bar AND (baz OR boffo))", which have been sybollically replaced by
### _replace_parens(), are restored as branches on the parse tree.
##############################################################################
sub _grow_branches {
    my $self = shift;
    my $sprout = shift;
    my $trunk = shift;

    return unless %{ $self->{paren_storage} };

    if ($sprout->{prodtype} eq 'paren' and !defined $sprout->{productions}) {
         my $labelreg= $self->{labelreg}; 
         $sprout->{qstring} =~ /($labelreg)/ 
             or confess "Internal error"; 
         $sprout->{productions} ||= [];
         push @{ $sprout->{productions} }, (delete $self->{paren_storage}{$1});
    }
    return unless defined $sprout->{productions};
    $self->_grow_branches($_, $trunk) for @{ $sprout->{productions} };
}

##############################################################################
### Find any double-quoted phrases and represent them with a symbolic name
### containing no space characters.
##############################################################################
sub _replace_phrases {
    my $self = shift;
    my $sprout = shift;
    my $trunk = shift;

    return unless $trunk->{-allow_phrases};

    my $randstring = $self->{randstring};

    while ($sprout->{qstring} =~ /$phrasereg/) {
        my $inc = ++$self->{phrase_inc};
        my $label = "__phrase$randstring" . sprintf('%04d', $inc);
        $sprout->{qstring} =~ s/$phrasereg/$label/;
        $self->{phrase_storage}{$label } = 
            {
                qstring => $1, # capture defined in $phrasereg
                prodtype => 'phrase',
                set_size => 0,
            };
    }
}

##############################################################################
### Represent all parenthetical statements with a symbolic name containing no
### space characters.  Start with the most deeply nested parenthetical
### statement and work outwards recursively.
##############################################################################
sub _replace_parens {
    my $self = shift;
    my $sprout = shift;
    my $trunk = shift;

    return unless $trunk->{-allow_boolean};
    
    my $randstring = $self->{randstring};
    
    while ($sprout->{qstring} =~ /$parenreg/) {
        my $inc = ++$self->{paren_inc};
        my $label = "__paren$randstring" . sprintf('%04d', $inc);
        $sprout->{qstring} =~ s/$parenreg/$label/;
        $self->{paren_storage}{$label} =  
            {
                qstring => $1, # capture defined in $parenreg
                prodtype => 'none',
                set_size => 0,
            };
    }
    
    return unless defined $sprout->{productions};
    $self->_replace_parens($_, $trunk) foreach @{ $sprout->{productions} };
}

##############################################################################
### Examine individual items and label them.  
### Tokenize multi-term groups to the extent that $tokenreg allows.
##############################################################################
sub _define_and_tokenize {
    my $self = shift;
    my $sprout = shift;
    my $trunk = shift;

    if (!$trunk->{-tokenize}) {
        $trunk->{prodtype} = 'terminal';
        return;
    }

    if (defined $sprout->{productions}) {
        $self->_define_and_tokenize($_, $trunk) for @{ $sprout->{productions} };
    }
    
    return if $sprout->{prodtype} eq 'phrase';
    return if $sprout->{prodtype} eq 'multi';
    return if $sprout->{prodtype} eq 'boolgroup';
    
    my $tokenreg = $self->{tokenreg};
    my $labelreg = $self->{labelreg};
    
    if ($sprout->{qstring} =~ /^\s*$boolopreg\s*$/) {
        $sprout->{prodtype} = 'boolop';
    }
    elsif ($sprout->{qstring} =~ /^\s*$unopreg?($labelreg)\s*$/) {
        my $prodtype = $1;
        $prodtype =~ s/__([a-z]+).*/$1/;
        $sprout->{prodtype} = $prodtype;
    }
    elsif ($sprout->{qstring} =~ /^\s*$unopreg?$tokenreg\s*$/) {
        $sprout->{prodtype} = 'tokenizable';
    }
    else {
        $sprout->{prodtype} = 'multi';
        $sprout->{set_size} = 0;
        my $qstring = $sprout->{qstring};
        
        my @matched = $qstring =~ 
            /.*?($unopreg?(?:$boolopreg|$labelreg|$tokenreg))/g;
         
        for my $candidate (@matched) {
            my $prodtype = ($candidate =~ /^$boolopreg$/) ?
                'boolop' : ($candidate =~ /^$unopreg?$labelreg$/) ?
                'label'  : 'tokenizable';
            $prodtype = $prodtype ne 'label' ?
                $prodtype : ($candidate =~ /$unopreg?__([a-z]+)/) ?
                $1 : 'error';
                
            $sprout->{productions} ||= [];
            push @{ $sprout->{productions} }, 
                {
                    qstring => $candidate,
                    prodtype => $prodtype,
                    set_size => 0,
                };
        }
    }
}

##############################################################################
### Group items joined by a boolean operator.
### e.g. "foo bar OR baz" becomes "foo (bar OR baz)" 
##############################################################################
sub _isolate_boolgroups {
    my $self = shift;
    my $sprout = shift;
    my $trunk = shift;
    
    return unless $trunk->{-allow_boolean};
    return unless defined $sprout->{productions};

    my $randstring = $self->{randstring};

    LOOP: {
        my $num_prods = @{ $sprout->{productions} };
        ### Don't bother with malformed groups like "foo OR".
        if ($num_prods >= 3) {
            foreach my $prodnum (1 .. ($num_prods-2)) {
                my $prod = $sprout->{productions}[$prodnum];
                next unless $prod->{prodtype} eq 'boolop';
                ### If the group already consists solely of "a BOOLOP b", 
                ### just label it.
                if ($num_prods == 3 and $prodnum == 1) {
                    $sprout->{prodtype} = 'boolgroup';
                    last;
                }
                ### Otherwise, create a branch for it in the parse tree.
                my $first_element = (($prodnum - 1) >= 0) ? ($prodnum-1) : 0; 
                my $length = ($first_element + 2) < $num_prods ? 3 : 
                             ($first_element + 1) < $num_prods ? 2 : 1; 
                my $inc = ++$self->{paren_inc};
                my $boolgrouplabel = "__boolgroup$randstring" 
                    . sprintf('%04d', $inc);
                my $boolgroup = {
                    productions => [],
                    prodtype    => 'boolgroup',
                    qstring     => $boolgrouplabel,
                    set_size    => 0,
                };
                @{ $boolgroup->{productions} } 
                    = splice(@{ $sprout->{productions} }, $first_element,
                        $length, $boolgroup);
                ### redo works where a while loop wouldn't: 
                ### "foo OR bar OR baz"
                redo LOOP;
            }
        }
    }
    $self->_isolate_boolgroups($_, $trunk) for @{ $sprout->{productions} };
}

##############################################################################
### Break up qstrings into tokens as determined by the language-specific
### tokenize_method.
##############################################################################
sub _tokenize_for_real {
    my $self = shift;
    my $sprout = shift;
    my $trunk = shift;

    return unless $trunk->{-tokenize};
    
    if (defined $sprout->{productions}) {
        $self->_tokenize_for_real($_, $trunk) for @{ $sprout->{productions} };
    }
    
    return unless $sprout->{prodtype} eq 'tokenizable';
    
    my $tokenreg = $self->{tokenreg};
    my $tokenize_method = $self->{tokenize_method};
    (my $tokenizable) = $sprout->{qstring} =~ /($tokenreg)/;
    my ($tokenized, undef) = $self->$tokenize_method( $tokenizable );
    if ($tokenized->[0] eq $sprout->{qstring}) {
        $sprout->{prodtype} = 'terminal'
    }
    elsif ($tokenized->[0] eq $tokenizable) {
        $sprout->{prodtype} = 'token';
        $sprout->{productions} = 
            [
                {
                    qstring  => $tokenized->[0],
                    prodtype => 'terminal',
                },
            ];
    }
    elsif ($#$tokenized > 0) {
        ($tokenized, undef) = $self->$tokenize_method( $self->{qstring} );
        $sprout->{prodtype} = 'multi';
        $sprout->{productions} ||= [];
        foreach my $freshtoken (@$tokenized) {
            push @{ $sprout->{productions} },
                {
                    qstring  => $freshtoken,
                    prodtype => 'terminal',
                };
        }
    }
}

##############################################################################
### Apply boolean operators, assiging values for required and negated to the
### adjacent productions.
##############################################################################
sub _apply_boolops {
    my $self = shift;
    my $sprout = shift;
    my $trunk = shift;

    return unless $trunk->{-allow_boolean};
    return unless defined $sprout->{productions};

    my $num_prods = @{ $sprout->{productions} };
    if ($num_prods > 2) {
        for (1 .. ($num_prods - 2)) {
            my $prod = $sprout->{productions}[$_];
            if ($prod->{prodtype} eq 'boolop') {
                my ($req_pre, $neg_pre, $req_post, $neg_post) 
                    = $prod->{qstring} =~ /AND NOT/ ?
                      (1,0,0,1) : $prod->{qstring} =~ /AND/ ?
                      (1,0,1,0) : $prod->{qstring} =~ /OR/  ?
                      (0,0,0,0) : (undef, undef, undef, undef);
                $sprout->{productions}[$_-1]{required} = $req_pre;
                $sprout->{productions}[$_-1]{negated}  = $neg_pre;
                $sprout->{productions}[$_+1]{required} = $req_post;
                $sprout->{productions}[$_+1]{negated}  = $neg_post;
            }
        }
    }
    $self->_apply_boolops($_, $trunk) for @{ $sprout->{productions} };
}

##############################################################################
### Apply prepended +plus and -minus, any_or_all
##############################################################################
sub _derive_required_and_negated {
    my $self = shift;
    my $sprout = shift;
    my $trunk = shift;
    
    ### This line cannot be uncommented, as this routine has to run
    ### regardless, in order to apply any_or_all.
    ### Resist the temptation to uncomment it!
    # return unless $trunk->{-allow_boolean}
    return unless defined $sprout->{productions};

    foreach my $prod (@{ $sprout->{productions} }) {
        if ($prod->{prodtype} eq 'boolop') {
            $prod->{required} = 0;
            $prod->{negated}  = 0;
            next;
        }
        
        if (!$trunk->{-allow_boolean}) {
            $prod->{required} = $trunk->{-any_or_all} eq 'all' ? 1 : 0;
            $prod->{negated}  = 0;
            next;
        }
        
        if ($prod->{prodtype} eq 'terminal') {
            next if defined $prod->{required};
            $prod->{required} = $trunk->{-any_or_all} eq 'all' ? 1 : 0;
            $prod->{negated} = 0;
        }
        elsif ($prod->{prodtype} eq 'token'
            or $prod->{prodtype} eq 'paren'
            or $prod->{prodtype} eq 'boolgroup'
            or $prod->{prodtype} eq 'phrase'
        ) 
        {
            if ($prod->{qstring} =~ /($unopreg)/) {
                my $unop = $1;
                ($prod->{required}, $prod->{negated}) = ($unop eq '+') ?
                    (1,0) : (0,1);
            }
            ### This comes into play if the production is 
            ### preceded by "AND NOT"
            if (defined $prod->{negated} and $prod->{negated}) {
                $prod->{required} = 0;
            }
            elsif (!defined $prod->{required}) {
                $prod->{required} = $trunk->{-any_or_all} eq 'all' ? 1 : 0;
                $prod->{negated} = 0;
            }
        }
        elsif ($prod->{prodtype} eq 'multi') {
            $prod->{required} = $trunk->{-any_or_all} eq 'all' ? 1 : 0;
            $prod->{negated}  = 0;
        }
    }
    
    $self->_derive_required_and_negated($_, $trunk) 
        for @{ $sprout->{productions} };
}

##############################################################################
### Lowercase if indicated.
##############################################################################
sub _apply_case_folding {
    my $self = shift;
    my $sprout = shift;
    my $trunk = shift;

    return unless $trunk->{-lowercase};
    
    $self->_apply_case_folding($_, $trunk) for @{ $sprout->{productions} };
    return unless $sprout->{prodtype} eq 'terminal';
    $sprout->{qstring} = lc($sprout->{qstring});
}

##############################################################################
### Stem if indicated.
##############################################################################
sub _apply_stemming {
    my $self = shift;
    my $sprout = shift;
    my $trunk = shift;

    return unless $trunk->{-stem};
    
    if (defined $self->{productions}) {
        $self->_apply_stemming($_, $trunk) for @{ $sprout->{productions} };
    }
    
    return unless $sprout->{prodtype} eq 'terminal';
    my $stem_method = $self->{stem_method};
    my $stemmed = $self->$stem_method( [ $sprout->{qstring} ] );
    $sprout->{qstring} = $stemmed->[0]
        if $stemmed->[0];
}

##############################################################################
### Parse previously isolated phrases.
##############################################################################
sub _expand_phrases {
    my $self = shift;
    my $sprout = shift;
    my $trunk = shift;

    return unless $trunk->{-allow_phrases};

    if (defined $sprout->{productions}) {
        $self->_expand_phrases($_, $trunk) for @{ $sprout->{productions} };
    }
    
    return unless $sprout->{prodtype} eq 'phrase';
    
    my $tokenize_method = $self->{tokenize_method};
    my $stem_method = $self->{stem_method};

    my $labelreg = $self->{labelreg};

    my ($label) = $sprout->{qstring} =~ /($labelreg)/ 
        or confess "Internal error";  
    my $original_text = $self->{phrase_storage}{$label}{qstring};
    my ($tokens, undef) = $self->$tokenize_method( $original_text );
    $tokens = $self->$stem_method( $tokens ) 
        if $trunk->{-stem};
    my @terminals;
    if (@$tokens > 1) {
        for (1 .. $#$tokens) {
            push @terminals, ($tokens->[$_-1] . ' ' . $tokens->[$_]);
        }
    }
    elsif (@$tokens == 1) {
        if (exists $self->{stoplist}{"$original_text"}) {
            push @terminals, '__STOPWORD__';
            $self->{status_hash}{stopwords}{$original_text} = undef;
        }
        else {
            push @terminals, $tokens->[0];
        }
    }
    else {
        ### TODO make sure that _recursive_score knows not to retrieve
        ### anything for a blank string.
        push @terminals, '';
    }

    my @productions;
    foreach my $terminal (@terminals) {
        push @productions, {
                qstring => $terminal,
                prodtype => 'terminal',
                required => 1,
                negated  => 0,
            };
    }
    $sprout->{productions} = \@productions;
}

__END__

=head1 NAME

Search::Kinosearch::QueryParser - parse search queries

=head1 SYNOPSIS

No public interface.

=head1 DESCRIPTION

This is a helper module for for Search::Kinosearch.  Do not use it by itself.  

=begin comment

=head2 new()

Private.

=head2 parse()

Private.

=end comment

=head1 AUTHOR

Marvin Humphrey <marvin at rectangular dot com>

=head1 COPYRIGHT

Copyright (c) 2005 Marvin Humphrey.  All rights reserved.
This module is free software.  It may be used, redistributed and/or 
modified under the same terms as Perl itself.

=cut
