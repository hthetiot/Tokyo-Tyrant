#! /usr/bin/perl

use strict;
use warnings;

use Cache::Memcached::Fast;

my $memd = Cache::Memcached::Fast->new({
    servers => [{ address => "localhost:1978", noreply => 1 }],
    connect_timeout => 10,
});

$memd->flush_all();
foreach my $i (1..100){
    my $rnd = int(rand(10));
    my $key = int(rand(100));
    if($rnd == 0){
      $memd->add($key, "[add:$i]");
    } elsif($rnd == 1){
      $memd->replace($key, "[replace:$i]");
    } elsif($rnd == 2){
      $memd->append($key, "[append:$i]");
    } elsif($rnd == 3){
      $memd->prepend($key, "[prepend:$i]");
    } elsif($rnd == 4){
      $memd->delete($key);
    } elsif($rnd == 5){
      $memd->incr($key, 1);
    } elsif($rnd == 6){
      $memd->decr($key, 1);
    } else {
      $memd->set($key, "[set:$i]");
    }
}

foreach my $i (1..100){
    my $val = $memd->get($i, $i);
    printf("%d: %s\n", $i, defined($val) ? $val : "(undef)");
}
