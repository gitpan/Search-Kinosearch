use Test::More;
use strict;
use warnings;

sub choppit {
    my $self = shift;
    my @out = @_;
    for (0 .. $#out) {
        chop $out[$_];
    }
    return \@out;
}

sub split_by_colon {
    my $self = shift;
    my $input = shift;
    return ([ split /:/, $input ], [ 0 ]);
}

### The following battery of tests applies to Search::Kinosearch and to
### Search::Kinosearch::Kindexer and Search::Kinosearch::KSearch, both of 
### which inherit from Search::Kinosearch.
sub test_language_specific_import {
    my $object = shift;
    
    can_ok($object,'define_language_specific_functions');
    
    $object->{language} = 'Xx';
    dies_ok { $object->define_language_specific_functions }
    "Invalid -language parameter croaks...";
    
    $object->{language} = 'En';
    $object->define_language_specific_functions;
    
    ok(exists $object->{stoplist}{the}, "Stoplist successfully imported");
    
    my $tokenreg = $object->{tokenreg};
    ok(("stuff" =~ /^$tokenreg$/ 
        and "don't" =~ /^$tokenreg$/
        and 'x x' !~ /^$tokenreg$/),
    "tokenreg successfully imported");

    my $tokenize = $object->{tokenize_method};
    my ($tokenized, $positions) = &$tokenize($object, 'flip-flops');
    is_deeply($tokenized, [ qw(flip flops) ], 
            "Imported tokenizer works properly...");

    my $stemmify = $object->{stem_method};
    my $stemmed = &$stemmify($object, [ 'flip', 'flops' ] );
    is_deeply($stemmed, [ qw(flip flop) ], 
            "Imported stemmer works properly");
        
    can_ok($object, 'set_tokenizer');
    dies_ok { $object->set_tokenizer('foo') } 
        "set_tokenizer requires a reference to a subroutine...";
        
    $object->set_tokenizer(\&split_by_colon);
    $tokenize = $object->{tokenize_method};
    ($tokenized, $positions) = &$tokenize($object, 'three:blind:mice');
    is_deeply($tokenized, [ qw(three blind mice) ], 
        "Setting an alternative tokenizer should work...");

    can_ok($object, 'set_stemmer');
    
    dies_ok { $object->set_stemmer('foo') } 
        "set_stemmer requires a reference to a subroutine...";
        
    $object->set_stemmer(\&choppit);
    $stemmify = $object->{stem_method};
    $stemmed = &$stemmify($object, @$tokenized);
    is_deeply($stemmed, [ qw(thre blin mic) ], 
        "Setting an alternative stemmer should work...");
    
    ### Reset the object to the proper state.
    $object->define_language_specific_functions;
}

1;