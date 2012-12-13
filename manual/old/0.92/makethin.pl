#! /usr/bin/env perl

# Make a thinner version of a LaTeX font
# By Scott Pakin <scott+st@pakin.org>

use File::Basename;
use Getopt::Long;
use Pod::Usage;
use Pod::Man;
use warnings;
use strict;

# Define some global variables.
my $progname = basename $0;      # Name of this program
my @fontlist;                    # List of fonts to make thinner
my @skipfonts;                   # Fonts specified by the user to ignore
my @extrafonts;                  # Additional fonts specified by the user
my @extramaps;                   # Additional .map files specified by the user
my @configexts;                  # List of config.* files to process
my @mapfiles;                    # List of .map files to copy and modify
my @megamap;                     # Entire contents of all .map files
my $cleanfirst;                  # Delete *.{tfm,vf} before creating new ones
my $cleanonly;                   # Same as $cleanfirst, but exit after deleting
my $dvifile;                     # Name of input .dvi file
my $xscale = 0.5;                # New font width as a fraction of the original
my $verbose = 0;                 # 1=output task info; >1=output file info
my $base = "thin";               # Name to use for config.* and *.map
my $also_pdftex = 0;             # 1=also produce a pdftex.map

###########################################################################

# Set or change a filename's extension.
sub set_ext ($$)
{
    my ($name, $path, $suffix) = fileparse ($_[0], '\.[^.]*');
    $path="" if $path eq "./";
    return $path . $name . $_[1];
}


# Try to find a file on disk using kpsewhich.  Extra arguments to
# kpsewhich can be included, too.
sub find_file (@)
{
    chomp (my $result = `kpsewhich @_`);
    return $? ? undef : $result;
}


# Make a font thinner.
sub thin_font ($$$$)
{
    my ($fname, $fpath, $fsuffix, $only_fontnames) = @_;
    my $virtualfont = $fsuffix eq ".vf";
    my $converter =  $virtualfont ? "vftovp" : "tftopl";
    open (ASCII_TEXT, "$converter $fpath$fname$fsuffix|") || die "open(): $!\n";

    # If we already converted the font, then merely look for additional fonts.
    if ($only_fontnames) {
        while (<ASCII_TEXT>) {
            /\(FONTNAME ([^\)]+)\)/i && push @fontlist, $1;
        }
        close ASCII_TEXT;
        return;
    }

    # Modify character widths.  Also, store the name of any additional font
    # we encounter in a virtual font file.
    my $ascii_file = $virtualfont ? "$fname.vpl" : "$fname.pl";
    open (ASCII_FILE, ">$ascii_file") || die "open(\"$ascii_file\"): $!\n";
    while (<ASCII_TEXT>) {
        /\(FONTNAME ([^\)]+)\)/i && push @fontlist, $1;
        s|^(\s*)\(CHARWD (\S+) ([^\)]+)\)|sprintf "%s(CHARWD %s %.7f)", $1, $2, $3*$xscale|gie;
        print ASCII_FILE $_;
    }
    close ASCII_FILE;
    close ASCII_TEXT;

    # Convert the result from ASCII to binary.
    if ($virtualfont) {
        system "vptovf $fname.vpl";
        die "system: $!\n" if $?;
        unlink "$fname.vpl";
    }
    else {
        system "pltotf $fname.pl";
        die "system: $!\n" if $?;
        unlink "$fname.pl";
    }
}


# Re-map a font to a thinner variant.
sub remap_font ($)
{
    # See if any map file maps the given font.
    my $fontname = $_[0];
    my @matches = grep {/^$fontname\s/} @megamap;
    return undef if $#matches==-1;

    # One does -- scale the font as specified.
    my $quoted = "";
    while ($matches[0] =~ s/\"([^\"]+)\"//) {
        $quoted .= $1 . " ";
    }
    $matches[0] =~ s/\s+/ /g;
    $quoted =~ s/\S+\s+ExtendFont//g;
    $quoted .= "$xscale ExtendFont";
    return $matches[0] . " \"$quoted\"";
}


# Output the embedded POD documentation in either *roff or PostScript format.
sub pod2man ($$)
{
    my ($manfile, $extraformatting) = @_;

    # Create an ordinary man page.
    my $parser = Pod::Man->new (center  => "",
                                date    => "17 July 2004",
                                release => "");
    $parser->parse_from_file ($0, $manfile);

    # If requested, perform some extra formatting to make the
    # documentation print nicer.
    return if !$extraformatting;
    my @formattedman;
    open (MANPAGE, "<$manfile") || die "open(\"$manfile\"): $!\n";
    while (<MANPAGE>) {
        s/LaTeX/L\\h'-0.36m'\\v'-0.15'\\s-2A\\s+2\\v'0.15'\\h'-0.15m'TeX/g;
        s/TeX/T\\h'-0.1667m'\\v'0.20'E\\v'-0.20'\\h'-0.125m'X/g;
        s/\\\*\(--/--/g;
        push @formattedman, $_;
    }
    close MANPAGE;
    open (MANPAGE, "| groff -man > $manfile") || die "open(\"$manfile\"): $!\n";
    print MANPAGE @formattedman;
    close MANPAGE;
}

###########################################################################

# Parse the command line.
my $wanthelp = 0;
Getopt::Long::Configure ("bundling");
GetOptions ("h|help"         => \$wanthelp,
            "x|xscale=f"     => \$xscale,
            "b|basename=s"   => \$base,
            "k|skipfonts=s"  => \@skipfonts,
            "e|extrafonts=s" => \@extrafonts,
            "m|extramaps=s"  => \@extramaps,
            "P|config=s"     => \@configexts,
            "c|clean"        => \$cleanonly,
            "C|cleanfirst"   => \$cleanfirst,
            "p|pdftex"       => \$also_pdftex,
            "man=s"          => sub {pod2man ($_[1], 0); exit 0},
            "man-ps=s"       => sub {pod2man ($_[1], 1); exit 0},
            "v|verbose+"     => \$verbose) || pod2usage (-verbose => 0,
                                                         -exitval => 1);
pod2usage (-verbose => $verbose,
           -exitval => 0) if $wanthelp;
pod2usage (-verbose => 0,
           -exitval => 1) if $#ARGV==-1;
$dvifile = $ARGV[0];

# Get a list of fonts from the DVI file and from the command line.
print "Acquiring font names from $dvifile\n" if $verbose;
open (DVI, "dvitype $dvifile|") || die "open(): $!\n";
my %uniquefonts;
while (<DVI>) {
    /Font \d+: (\w+)/ && do {$uniquefonts{$1}=1};
}
close DVI;
foreach (map {split /,/, $_} @extrafonts) {
    $uniquefonts{$_} = 1;
}
@fontlist = sort {$a cmp $b} keys %uniquefonts;
my %skipfonts = map {($_ => 1)} map {split /,/, $_} @skipfonts;

# If told to, delete generated files from previous runs.
if ($cleanfirst || $cleanonly) {
    print "Deleting generated font files, configuration files, and font maps\n" if $verbose;
    my @old_fontlist = @fontlist;
    while (@fontlist) {
        my $fontname = shift @fontlist;
        if ($skipfonts{$fontname}) {
            print "   Ignoring $fontname (as directed by the command line)\n" if $verbose>1;
            next;
        }
        thin_font ($fontname, "", ".vf", 1) if -e "$fontname.vf";
        foreach my $fsuffix (".vf", ".tfm") {
            if (-e "$fontname$fsuffix") {
                print "   Removing $fontname$fsuffix\n" if $verbose>1;
                unlink "$fontname$fsuffix";
            }
        }
    }
    my @extra_deletes =  ("config.$base", "$base.map");
    push (@extra_deletes, "pdftex.cfg") if $also_pdftex;
    foreach (@extra_deletes) {
        if (-e $_) {
            print "   Removing $_\n" if $verbose>1;
            unlink $_;
        }
    }
    exit 0 if $cleanonly;
    @fontlist = @old_fontlist;
}

# Convert each font in turn.
print "Creating *.tfm and *.vf files with ${xscale}X horizontal scaling\n" if $verbose;
my %allfonts;
while (@fontlist) {
    my $fontname = shift @fontlist;
    if ($skipfonts{$fontname}) {
        print "   Ignoring $fontname (as directed by the command line)\n" if $verbose>1;
        next;
    }
    $allfonts{$fontname} = 1;
    if (-e (set_ext $fontname, ".vf") || -e (set_ext $fontname, ".tfm")) {
        print "   Skipping $fontname (already converted)\n" if $verbose>1;
        if (-e set_ext $fontname, ".vf") {
            $fontname .= ".vf";
        }
        else {
            $fontname .= ".tfm";
        }
        my ($fname, $fpath, $fsuffix) = fileparse $fontname, "\.[^.]*";
        thin_font ($fname, $fpath, $fsuffix, 1);   # Only search for new fonts.
    }
    else {
        # Use a .vf file if available.  If not, then use the .tfm file.
        print "   Processing $fontname\n" if $verbose>1;
        my ($fname, $fpath, $fsuffix);
        if (find_file "$fontname.vf") {
            ($fname, $fpath, $fsuffix) = fileparse find_file ("$fontname.vf"), ".vf";
        }
        elsif (find_file "$fontname.tfm") {
            ($fname, $fpath, $fsuffix) = fileparse find_file ("$fontname.tfm"), ".tfm";
        }
        else {
            die "${progname}: I don't know anything about $fontname\n";
        }

        # Convert the font.
        thin_font ($fname, $fpath, $fsuffix, 0);
    }
}

# Acquire a list of .map files to process.
print "Reading dvips configuration files (config.*)\n" if $verbose;
foreach my $ext ("ps", @configexts) {
    my $configfile = find_file '--format="dvips config"', "config.$ext";
    die "${progname}: unable to find config.$ext\n" if !$configfile;
    print "   Searching for map files in $configfile\n" if $verbose>1;
    open (CONFIGFILE, "<$configfile") || die "open(\"$configfile\"): $!\n";
    while (<CONFIGFILE>) {
        # Search for a PostScript font alias filename.
        chomp;
        s/\%.*//;
        next if !/^\s*p\s+(\+?)(\S+)/;

        # We found it -- adjust the list of .map files accordingly.
        @mapfiles=() if $1 eq "";
        my $map = find_file '--format="dvips config"', $2;
        die "${progname}: unable to find $2\n" if !$map;
        push @mapfiles, $map;
    }
    close CONFIGFILE;
}

# Merge the contents of all map files into @megamap.
print "Reading font maps (*.map)\n" if $verbose;
foreach my $mapfile ((map {split /,/, $_} @extramaps), @mapfiles) {
    print "   Reading font mappings from $mapfile\n" if $verbose>1;
    open (MAPFILE, "<$mapfile") || die "open(\"$mapfile\"): $!\n";
    chomp (my @mappings = <MAPFILE>);
    close MAPFILE;
    push @megamap, @mappings;
}

# Create a dvips configuration file.
print "Generating config.$base\n" if $verbose;
open (CONFIG, ">config.$base") || die "open(\"config.$base\"): $!\n";
print CONFIG <<"CONFIG_EOF";
% This file can be freely modified.  It can also be
% redistributed, provided that it is not called "config.thin".
p +$base.map
CONFIG_EOF
close CONFIG;

# Create a dvips map file.
print "Generating $base.map\n" if $verbose;
open (FONTMAP, ">$base.map") || die "open(\"$base.map\"): $!\n";
print FONTMAP <<'FONTMAP_EOF';
% This file can be freely modified.  It can also be
% redistributed, provided that it is not called "thin.map".
FONTMAP_EOF
my $num_maps = 0;
foreach my $fontname (sort {$a cmp $b} keys %allfonts) {
    my $remapping = remap_font $fontname;
    if ($remapping) {
        print FONTMAP $remapping, "\n";
        print "   Wrote mapping for $fontname\n" if $verbose>1;
        $num_maps++;
    }
    elsif (! -e "$fontname.vf") {
        warn "${progname}: warning: no mapping was found for $fontname\n";
    }
}
close FONTMAP;
warn "${progname}: warning: no fonts were written to $base.map\n" if !$num_maps;

# Create a pdfTeX config file.
if ($also_pdftex) {
    print "Generating pdftex.cfg\n" if $verbose;
    unlink "pdftex.cfg";
    my $pdfcfg = find_file "--progname=pdftex", "pdftex.cfg";
    die "${progname}: unable to find pdftex.cfg\n" if !$pdfcfg;
    open (INCONFIG, "<$pdfcfg") || die "open(\"$pdfcfg\"): $!\n";
    open (OUTCONFIG, ">pdftex.cfg") || die "open(\"pdftex.cfg\"): $!\n";
    while (<INCONFIG>) {
        next if /^\s*map\s/;       # Discard all map lines.
        print OUTCONFIG $_;
    }
    print OUTCONFIG "\n% The next line was added by $progname.\n";
    print OUTCONFIG "map $base.map\n";
    close OUTCONFIG;
    close INCONFIG;
}

###########################################################################

__END__

=head1 NAME

makethin - make thinner versions of PostScript fonts for TeX


=head1 SYNOPSIS

makethin
[B<--verbose>]
[B<--xscale>=I<factor>]
[B<--cleanfirst>]
[B<--clean>]
[B<--config>=I<extension>]
[B<--pdftex>]
[B<--extramaps>=I<file>[,I<file>]...]
[B<--skipfonts>=I<font>[,I<font>]...]
[B<--basename>=I<string>]
[B<--extrafonts>=I<font>[,I<font>]...]
I<.dvi file>

makethin
[B<--verbose>]
B<--help>

makethin
B<--man>=I<man page> | B<--man-ps>=I<PostScript file>



=head1 DESCRIPTION

B<makethin> produces thinner versions of PostScript fonts for use with
TeX/LaTeX and Dvips.  More precisely, it finds all of the F<.tfm>
and F<.vf> fonts referred to by a F<.dvi> file, scales the
character-width metrics by a given amount, and writes new F<.tfm> and
F<.vf> files to the current directory.  B<makethin> then generates a
customized F<.map> and F<config> file for Dvips and, optionally, a
customized F<pdftex.cfg> file for pdfLaTeX.

The general procedure for using B<makethin> is as follows:

=over 4

=item 1.

Run B<latex> on your F<.tex> source file to produce a F<.dvi> file.

=item 2.

Run B<makethin> on the F<.dvi> file to create new F<.tfm>, F<.vf>,
F<.map>, and F<config> files.

=item 3.

Re-run B<latex> on your F<.tex> source file to typeset it with the new
fonts.

=item 4.

Run B<dvips> on the F<.dvi> file, specifying the newly generated
F<.map> and F<config> files, to produce a F<.ps> file.

=back


=head1 OPTIONS

The following are the command-line options that B<makethin> accepts:

=over 4

=item B<-v>, B<--verbose>

Increase the verbosity of the status output.  B<--verbose> can be
specified multiple times on the same command line, with each
B<--verbose> futher increasing the verbosity.  (Currently, two
B<--verbose>s have maximal impact.)

=item B<-h>, B<--help>

Display basic usage information.  When combined with B<--verbose>,
additionally describes each of the command-line options.  When
combined with a second B<--verbose>, B<--help> outputs the complete
B<makethin> manual page.

=item B<-x> I<factor>, B<--xscale>=I<factor>

Scale fonts horizontally by a factor of I<factor>.  The default,
C<0.5>, produces nearly illegible fonts but is useful for verifying
that B<makethin> actually worked.  Factors of C<0.90>-C<0.99> are more
reasonable.

=item B<-C>, B<--cleanfirst>

Delete all files generated by a previous run of B<makethin> before
generating new ones.

=item B<-c>, B<--clean>

Delete all files generated by a previous run of B<makethin> and then
exit.

=item B<-P> I<extension>, B<--config>=I<extension>

Process all of the F<.map> files named in F<config.>I<extension>
(found in Dvips's configuration directory).  B<--config> can be
specified multiple times on the same command line.  F<config.ps> is
processed implicitly.  The same B<-P> arguments that you would
normally pass to B<dvips> to utilize entirely PostScript fonts (e.g.,
B<-Pcmz> and B<-Pamz>) should also be passed to B<makethin>.

=item B<-p>, B<--pdftex>

In addition to producing the files needed by Dvips, also produce a
F<pdftex.cfg> file that is usable by pdfLaTeX.

=item B<-m> I<file>[,I<file>]...], B<--extramaps>=I<file>[,I<file>]...]

Additionally process the named Dvips F<.map> files even if they're
not referred to by any of the F<config.>I<extension> files specified
with B<--config>.  B<--extramaps> can be specified multiple times on
the same command line.

=item B<-k> I<font>[,I<font>]...], B<--skipfonts>=I<font>[,I<font>]...]

Don't make thin versions of the named fonts, even if they're listed in
the F<.dvi> file.  Fonts are named using the Berry scheme (i.e.,
``C<pcrr8a>'' as opposed to ``C<COURB>'' or ``C<Courier-Bold>'').
B<--skipfonts> can be specified multiple times on the same command
line.

=item B<-b> I<string>, B<--basename>=I<string>

Tell B<makethin> to use I<string> as the base name for the Dvips
configuration files it generates.  The default is ``C<thin>'', so
B<makethin> normally produces files named F<config.thin> and
F<thin.map>, but B<--basename> enables alternate filenames to be used.

=item B<-e> I<font>[,I<font>]...], B<--extrafonts>=I<font>[,I<font>]...]

Make thin versions of the named fonts, even if they're not listed in
the F<.dvi> file.  Fonts are named using the Berry scheme (i.e.,
``C<pcrr8a>'' as opposed to ``C<COURB>'' or ``C<Courier-Bold>'').
B<--extrafonts> can be specified multiple times on the same command
line.

=item B<--man>=I<man page>

Create a Unix manual page for B<makethin> in the standard, *roff
format.  Typical usage is:

    makethin --man=/usr/man/man1/makethin.1

=item B<--man-ps>=I<PostScript file>

Create a Unix manual page for B<makethin> in PostScript format instead
of *roff format.

=back

In addition to the options listed above, B<makethin> has a required
argument, which is the name of a F<.dvi> file from which to read font
information.


=head1 EXAMPLES

The following are some examples of how to use B<makethin>.

=head2 A typical case

First, we need to produce F<myfile.dvi>, because that contains the
font information that B<makethin> will read:

    latex myfile.tex

Next, we invoke B<makethin>, telling it to process F<config.cmz>
(which, in turn, causes F<psfonts.cmz> to be processed), This tells
B<makethin> to use PostScript versions of the Computer Modern fonts
instead of bitmapped versions.  (B<makethin> can scale only PostScript
fonts.)  We also specify maximal verbosity:

    makethin -Pcmz --verbose --verbose myfile.dvi

The preceding line reads F<config.ps>, F<config.cmz>, various map
files, such as F<psfonts.map> and F<psfonts.cmz>, and all of the
F<.tfm> and F<.vf> files mentioned in F<myfile.dvi>.  It then writes
F<config.thin>, F<thin.map>, and modified versions of all of the
F<.tfm> and F<.vf> files to the current directory.

We now need to re-run B<latex>, so it can produce a new F<myfile.dvi>
using the thinner metrics listed in the current directory's F<.tfm>
and F<.vf> files:

    latex myfile.tex

Finally, we produce a PostScript file using the newly generated using
F<config.thin> and F<thin.map> files:

    dvips -Pthin myfile.dvi -o myfile.ps

If all worked according to plan, F<myfile.ps> should be typeset using
extremely thin (half-width) versions of its original fonts.


=head2 Producing thin fonts for use in pdfLaTeX

Because B<makethin> can read only F<.dvi> files, not F<.pdf> files, we
first need to produce a F<.dvi> file:

    latex too-long.tex

F<too-long.dvi> is typeset entirely using the Times family of fonts.
Therefore, we don't need to specify B<-Pcmz>.  However, B<pdflatex>
normally embeds Times, thereby precluding B<makethin>'s ability to
scale it.  (B<makethin> requires a F<.pfb> font file in order to scale
the corresponding font.)  Fortunately,
F</usr/share/texmf/dvips/config/ar-std-urw-kb.map> already contains
the proper mapping of TeX names to F<.pfb> files for Times, Courier,
and Helvetica.  We can tell B<makethin> to use that file:

    makethin --cleanfirst -v -v too-long.dvi --pdftex --xscale=0.9
      --extramaps=/usr/share/texmf/dvips/config/ar-std-urw-kb.map

In the preceding line, we changed the scaling factor from the default
of 0.5 to a more reasonable 0.9.  Because we had some 0.5-scaled
F<.tfm> and F<.vf> files left over from the previous example, we
specified B<--cleanfirst> to delete those old font files.  We
specified B<--pdftex> to make B<makethin> produce a local
F<pdftex.cfg> file.  And we told B<makethin> where to find the extra
map file needed to force the usage of F<.pfb> files.

All that's left is to run B<pdflatex> to produce a F<.pdf> file:

    pdflatex too-long.tex

B<pdflatex> will read the font metric files (F<.tfm> and F<.vf>) and
F<pdftex.cfg> from the current directory.  This will tell it to load
F<thin.map>, which specifies the scaling factor.  The result should be
a document with each character squeezed to 90% of its original width.


=head1 FILES

=over 4

=item B<perl>

interpreter/compiler needed to run the B<makethin> script

=item B<kpsewhich>

finds files within the TeX directory tree

=item B<dvitype>

outputs the typesetting commands contained within a F<.dvi> file

=item F<*.tfm> and F<*.vf>

TeX font metrics and virtual fonts--metrics specifying the width of
each character in a font

=item F<config.*>

Dvips configuration files, each containing (among other
information) a list of font-map files

=item F<psfonts.*>, F<*.map>

Dvips font-map files, which map TeX font names to PostScript font
names and F<.pfb> files

=item F<pdftex.cfg>

pdfTeX and pdfLaTeX configuration files, each containing (among other
information) a list of font-map files

=item B<groff>

used by B<--man-ps> to produce a PostScript version of the Unix manual
page for B<makethin>

=back


=head1 RESTRICTIONS

The most serious restriction is that B<makethin> doesn't work on
Computer Modern Roman 10pt. (F<cmr10>)--the default TeX/LaTeX font.
The reason, I believe, is that TeX and LaTeX preload that font's
metrics (F<cmr10.tfm>) and therefore ignore the scaled F<cmr10.tfm> on
disk.  It may be possible to work around this limitation by copying
F<cmr10.tfm> to a new name and convincing LaTeX to use that name where
it would otherwise have used F<cmr10.tfm>.  However, it's much easier
merely to use a different font family (e.g., with
``C<\usepackage{times}>'') for typesetting your document when you know
you want to run B<makethin>.


=head1 SEE ALSO

dvips(1),
latex(1),
pdflatex(1),
the LaTeX C<savetrees> package


=head1 AUTHOR

Scott Pakin, I<scott+st@pakin.org>
