#! /usr/bin/perl

use strict;
use warnings;

use LWP::UserAgent;

my $err = 0;
my $ua = LWP::UserAgent->new(keep_alive => 1);
my $baseurl = 'http://localhost:1978/';

my $req = HTTP::Request->new(PUT => $baseurl . "%E5%B9%B3%E6%9E%97", [], "mikio");
my $res = $ua->request($req);
my $code = $res->code();
if($code != 201){
    printf("Error: %s\n", $res->status_line());
    $err = 1;
}

foreach my $i (1..100){
    $req = HTTP::Request->new(PUT => $baseurl . $i, [], "mikio:$i");
    $res = $ua->request($req);
    if($res->code() != 201){
        printf("Error: %s\n", $res->status_line());
        $err = 1;
    }
}

foreach my $i (1..10){
    my $pdmode = int(rand(3));
    $req = HTTP::Request->new(PUT => $baseurl . $i,
                              ["X-TT-PDMODE" => $pdmode], "mikio:$i");
    $res = $ua->request($req);
    $code = $res->code();
    if($code != 201 && $code != 409){
        printf("Error: %s\n", $res->status_line());
        $err = 1;
    }
}

for(my $i = 1; $i < 107; $i += 7){
    $req = HTTP::Request->new(DELETE => $baseurl . $i);
    $res = $ua->request($req);
    if($res->is_error() && $res->code() != 404){
        printf("Error: %s\n", $res->status_line());
        $err = 1;
    }
}

for(my $i = 1; $i < 100; $i += 3){
    $req = HTTP::Request->new(POST => $baseurl . $i,
                              ["X-TT-XNAME" => "echo", "X-TT-XOPTS" => 1], "hirabayashi:$i");
    $res = $ua->request($req);
}

for(my $i = 1; $i < 100; $i += 3){
    $req = HTTP::Request->new(POST => $baseurl . $i,
                              ["X-TT-MNAME" => "getlist", "X-TT-MOPTS" => 1], "1=$i&2=$i");
    $res = $ua->request($req);
}

foreach my $i (1..107){
    $req = HTTP::Request->new(GET => $baseurl . $i);
    $res = $ua->request($req);
    $code = $res->code();
    if($res->is_success()){
        printf("%d: %s\n", $i, $res->content());
    } elsif($res->code() != 404){
        printf("Error: %s\n", $res->status_line());
        $err = 1;
    }
    $req = HTTP::Request->new(HEAD => $baseurl . $i);
    $res = $ua->request($req);
    if($res->is_success()){
        printf("%d: %s\n", $i, $res->header("content-length"));
    } elsif($res->code() != 404){
        printf("Error: %s\n", $res->status_line());
        $err = 1;
    }
}

if($err){
    printf("finished with error\n");
    exit(1);
}

printf("finished successfully\n");
exit(1);
