#! /usr/bin/perl

use strict;
use warnings;

use constant {
    DICTFILE => '/usr/share/dict/words',
    HOSTNAME => 'localhost',
    PORT => 1978,
    TEXTNUM => 10000,
    WORDNUM => 1000,
};

my @words;
my %uniq;
my $ifp;
open($ifp, '<' . DICTFILE) || die("could not open the dictionary");
while(defined(my $word = <$ifp>)){
    chomp($word);
    $word =~ tr/A-Z/a-z/;
    next if($word =~ /[^a-z]/);
    next if(length($word) < 2);
    if($word =~ /s$/){
        my $singular = $word;
        $singular =~ s/s$//;
        next if(defined($uniq{$singular}));
    }
    my $num = 100 / length($word) + 1;
    for(my $i = 0; $i < $num; $i++){
        push(@words, $word);
    }
    $uniq{$word} = 1;
}
close($ifp);
my $rnum = scalar(@words);

$ENV{'PATH'} = $ENV{'PATH'} . ":.";
$| = 1;
printf("start\n");
for(my $i = 1; $i <= TEXTNUM; $i++){
    printf(".");
    my $text = $words[int(rand($rnum))];
    my $wnum = int(rand(WORDNUM * 2));
    for(my $j = 1; $j < $wnum; $j++){
        $text .= ' ' . $words[int(rand($rnum))];
    }
    my $cmd = sprintf("tcrmgr ext -port %d %s put %d '%s'", PORT, HOSTNAME, $i, $text);
    system($cmd . ">/dev/null");
    printf(" (%d)\n", $i) if($i % 50 == 0);
}
printf("finish\n");



# END OF FILE
