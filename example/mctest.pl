#! /usr/bin/perl

use strict;
use warnings;

use Cache::Memcached;

my $memd = Cache::Memcached->new();
$memd->set_servers(["localhost:1978"]);

$memd->flush_all();
foreach my $i (1..100){
    $memd->set($i, "mikio:$i");
}
foreach my $i (1..101){
    $memd->add($i, "hirabayashi:$i");
}
foreach my $i (1..100){
    $memd->replace($i, $i);
}
foreach my $i (1..100){
    $memd->incr($i, 100);
}
foreach my $i (1..100){
    $memd->decr($i, 10);
}
for(my $i = 1; $i < 100; $i += 7){
    $memd->delete($i);
}
foreach my $i (1..102){
    my $val = $memd->get($i, $i);
    printf("%d: %s\n", $i, defined($val) ? $val : "(undef)");
}
my $stats = $memd->stats();
while(my ($key, $value) = each(%$stats)){
    if(ref($value) eq 'HASH'){
        while(my ($tkey, $tvalue) = each(%$value)){
            printf("%s:%s:%s\n", $key, $tkey, $tvalue);
        }
    } else {
        printf("%s:%s\n", $key, $value);
    }
}
