package Search::Kinosearch::Tutorial

1;

__END__

=head1 NAME

Search::Kinosearch::Tutorial - Kinosearch Tutorial

=head1 DEPRECATED

Search::Kinosearch has been superseded by L<KinoSearch|KinoSearch>.  Please
use the new version.

=head1 SYNOPSIS

Documentation only.

=head1 DESCRIPTION

The following sample code for kindexer.plx and search.cgi can be used to
create a simple search engine.  It requires the html presentation of the US
Constitution included in the distribution for
L<Search::Kinosearch|Search::Kinosearch>.

=head2 kindexer.plx

    #!/usr/bin/perl
    
    ### kindexer.plx -- index a collection of html files.
    
    use strict;
    use warnings;
    
    use File::Spec;
    use Search::Kinosearch::Kindexer;
    
    ### In order for kindexer.plx to work, you must modify $sourcedir 
    ### and $mainpath.
    ###
    ### $sourcedir must lead to the directory containing the US 
    ### Constitution html files.
    ###
    ### $mainpath is the future location of the kindex.  
    my $sourcedir = 'sample_files/us_constitution';
    my $mainpath = '';
    
    ### STEP 1: Create a Kindexer object.
    my $kindexer = Search::Kinosearch::Kindexer->new(
        -mainpath => $mainpath,
        -mode     => 'overwrite',
        );
    
    ### STEP 2: Define all the fields that you'll ever need this kindex 
    ### to have -- because as soon as you process your first document, 
    ### you lose the ability to add, remove, or change the characteristics 
    ### of any fields.  
    $kindexer->define_field( 
        -name      => 'title',
        -lowercase => 1,
        -tokenize  => 1,
        -stem      => 1,   
        );
    $kindexer->define_field( 
        -name      => 'bodytext',
        -lowercase => 1,
        -tokenize  => 1,
        -stem      => 1,   
        );
    $kindexer->define_field( 
        -name => 'url',
        -score => 0,
        );
    
    opendir CONSTITUTION_DIR, $sourcedir
        or die "Couldn't open directory '$sourcedir': $!";
    my @files = readdir CONSTITUTION_DIR;
    closedir CONSTITUTION_DIR;
        
    foreach my $file (@files) {
        next unless  $file =~ /\.html/;
        next if $file eq 'index.html';
        my $path = File::Spec->catfile($sourcedir, $file);
        open FILE, $path or die "Couldn't open file '$path': $!";
    
        my $content;    
        {
            local $/; # slurp mode
            $content = <FILE>;
            close FILE;
        }
        
        my $title = $content;
        warn "Couldn't isolate title in '$path'" 
            unless $title =~ s#.+<title>(.+?)</title>.+#$1#s;
        
        warn "Couldn't isolate bodytext in '$path'"
            unless $content =~ s#.+<div id="bodytext">(.+?)</div>.+#$1#s;
            
        ### Quick and dirty tag stripping.
        ### This is usually unsafe, but since we know _exactly_ what is in 
        ### these html files, we can guarantee that it works.
        $content =~ s/<.*?>//gs; 
        
        my $url = "/us_constitution/$file";
        
        ### STEP 3: Start a new document, identified by something unique
        ### (such as a URL).
        my $doc = $kindexer->new_doc($url);
        
        ### STEP 4: Set the value for each field.
        $doc->set_field( url       => $url   );
        $doc->set_field( title     => $title );
        $doc->set_field( bodytext  => $content   );
        
        ### STEP 5 Add the document to the kindex.
        $kindexer->add_doc($doc);
    
        ### STEP 6 Repeat steps 3-5 for each document in the collection.
    }
    
    ### STEP 7 Finalize the kindex.
    $kindexer->generate;
    $kindexer->write_kindex;

=head2 search.cgi

    #!/usr/bin/perl -T
    
    ### search.cgi -- simple search application
    
    use strict;
    use warnings;
    
    use CGI;
    use Search::Kinosearch::KSearch;
    use Search::Kinosearch::Query;
    
    my $cgi = CGI->new;
    my $q = $cgi->param('q') || '';
    
    ### In order for search.cgi to work, $mainpath must be modified so 
    ### that it points to the 'kindex' created by kindexer.plx 
    my $mainpath = '';
    
    ### STEP 1: Create a KSearch object.
    my $ksearch = Search::Kinosearch::KSearch->new(
        -mainpath      => $mainpath,
        -excerpt_field => 'bodytext',
        -num_results   => 50,
        );
    
    ### STEP 2: Add a query to the KSearch object.
    my $query = Search::Kinosearch::Query->new(
        -string    => $q,
        -lowercase => 1,
        -tokenize  => 1,
        -stem      => 1,
        );
    $ksearch->add_query( $query );
    
    ### STEP 3: Process the search.
    $ksearch->process;
    
    ### STEP 4: Format the results however you like.
    my $report = '';
    while (my $hit = $ksearch->fetch_hit_hashref) {
        $report .= qq(
            <p class="gen">
                <a href="$hit->{url}">
                    <strong>$hit->{title}</strong>
                </a>
            <em>$hit->{score}</em>
            <br />
            $hit->{excerpt}
            </p>
            );
    }
    
    $q =~ s/"/&quot;/g;
    
    print "Content-type: text/html\n\n";
    print <<EOSTUFF;
    <!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN">
    
    <html>
    
    <head>
    
    <title>Kinosearch: $q</title>
    
    </head>
    
    <body bgcolor="#ffffff">
    
    <form action="">
    <h3 class="gen">Search the U.S. Constitution:</h3>
    <input type="text" name="q" id="q" value="$q">
    <input type="submit" value="search">
    </form>
    
    $report
    
    <p class="gen">
        <em>Powered by 
            <a href="http://www.rectangular.com/kinosearch/">
                Kinosearch
            </a>
        </em>
    </p>
    
    </body>
    
    </html>
    EOSTUFF

=head1 SEE ALSO

=over

=item

L<Search::Kinosearch|Search::Kinosearch>

=item

L<Search::Kinosearch::Kindexer|Search::Kinosearch::Kindexer>

=item

L<Search::Kinosearch::KSearch|Search::Kinosearch::KSearch>

=item

L<Search::Kinosearch::Query|Search::Kinosearch::Query>

=back

=head1 TO DO

Add advanced_kindexer.plx and advanced_search.cgi

=head1 AUTHOR

Marvin Humphrey <marvin at rectangular dot com>

=head1 COPYRIGHT

Copyright (c) 2005 Marvin Humphrey.  All rights reserved.
This module is free software.  It may be used, redistributed and/or 
modified under the same terms as Perl itself.

=cut

