#!/usr/bin/env perl

use IO::Socket;
use POSIX;
use bytes;
use strict;
use Data::Dumper;
use Config::IniFiles;
use Sys::Hostname;
use Digest::CRC qw(crcccitt crc crc8);
use Sys::Syslog qw(:standard);
use Fcntl qw(:flock);
use threads;
use threads::shared;

share(my %result);
my $read_sock;

use constant MODBUS_READ_HOLDING_REGISTERS                      => 0x03;

if(!open(LOCK,">/tmp/meters.lock")){
    print "Cannot open the lock file";
    exit 2;
}
if(!flock(LOCK,LOCK_EX|LOCK_NB)){
    print "Cannot lock the lock file";
    close(LOCK);
    exit 3;
}

openlog("meters", "ndelay,pid", "local0");

my $DEBUG = 1;

my $zabbix_sender = '/usr/bin/zabbix_sender';
my $timeout = "30";
my $z_ip = inet_ntoa((gethostbyname(hostname))[4]);

####################################
###Типы счетчиков
###Должны быть определены в главном цикле, а также в функциях:
###build_tx_frame
###rcv_buffer
####################################
my @types = ("ce102", "ce102m", "ce301", "ceb2", "umg103");

#parse config
my $config_file = "/usr/local/etc/meters.cfg";
my $cfg = new Config::IniFiles -file => "$config_file" or die "Error while opening config: $!";
my %c = %{$$cfg{'v'}};
my (%hosts, %commands);

while (my ($key, $value) = each(%c))
{
        if ($key =~ '-cmds')
        {
		foreach my $type (@types) {
			if ($key =~ $type) {
				#print "K $key T $type \n";
				for my $k (keys %$value)
		                {
                        		$commands{"commands_" . "$type"}{$k} = $$value{$k};
	       		        }
			}
		}
        }
        else
        {
                $hosts{$key} = $value;
        }
}


#print Dumper($commands{"commands_ce301"});
#exit;
#while (my ($key, $value) = each(%{"commands_ceb2"})){
#     print " K $key =>  V $value \n";
#}


########################
###main loop
########################
while (my ($key, $value) = each(%hosts))
{
	#################
	###ceb2a
	#################
	my $act_sum = 0;
	my $count_sum = 0;
	#################
	
	my $cmd_send = 0;
	my $cmd_recv = 0; 

        print "\nprocessing host: $key\n" if $DEBUG;
        my $proto = $$value{'proto'};
        my $ip = $$value{'ip'};
        my $port = $$value{'port'};
	my $type = $$value{'type'};

	my $serial = 0;
	if ($$value{'serial'}) 
	{
		$serial = $$value{'serial'};
	}

        print "socket param: ip $ip port $port proto $proto type $type\n" if $DEBUG;
	syslog("info", "hostname: $key socket param: ip $ip port $port proto $proto type $type");

        #create socket
        my $sock = new IO::Socket::INET (
                PeerAddr    => $ip,
                PeerPort    => $port,
                Proto        => $proto,
		Timeout 	=> 5
        ) or next;

	#########################
	###для отправки серийников CE102
	#########################
	if ($type eq "ce102")
        {
		send_to_zbx($key,"snumb0",$serial);
	}

	if (($key =~ "uniping")&&($type eq "ce301"))
	{
		fcntl($sock, F_SETFL, O_NONBLOCK) or die "Set socket param error: $!";
		$read_sock = threads->new(\&read_data, $sock);
		syslog("info", "start thread for uniping");
		%result = ();
	}

	if (($type eq "ce102m"))
        {
                fcntl($sock, F_SETFL, O_NONBLOCK) or die "Set socket param error: $!";
                $read_sock = threads->new(\&read_data, $sock);
                syslog("info", "start thread for ce102m");
                %result = ();
		################
		###start session
        	###############
	        my $cmd_ini = "/?!" . "\x0d" . "\x0a";
	        print "send first command => $cmd_ini \n" if $DEBUG;
        	send $sock,$cmd_ini,0;
	        select(undef, undef, undef, 0.4);
        	$cmd_ini = "\x06" . "051" . "\x0d" . "\x0a";
	        print "send confirm command => $cmd_ini \n" if $DEBUG;
        	send $sock,$cmd_ini,0;
	        select(undef, undef, undef, 0.4);
        }

	while (my ($cmd_key, $cmd_value) = each(%{$commands{"commands_" . "$type"}}))
	{
		if ($type eq "ce102") 
		{
			######################
			###length of data from rx_frame
			######################
			my $data_len = "8";
			if ($cmd_value == "50") 
			{
				$data_len = "6";
			}
			my $phase = 0;

			#####################
                        ###change phase
                        ######################
			if ($cmd_key eq "ener_t2")
			{
				$phase = "1";
			}

			$cmd_value = "\x01" . chr($cmd_value);
			my $tx_frame = build_tx_frame($type,$cmd_value,$serial,$phase);
			$cmd_send++;
			send $sock,$tx_frame,0;
			#select(undef, undef, undef, 0.4);
			#sleep(1);
			my $rx_frame = rcv_buffer($sock,$type,$serial);
			my $result = get_ce102_data($rx_frame,$data_len);
			print "RESULT: KEY $cmd_key FRAME => $result \n" if $DEBUG;
			$cmd_recv++;
			if ($result < 10000000)
			{
				send_to_zbx($key,$cmd_key,$result);
			}
			else
			{
				syslog("info", "ERROR: too long result for host => $key key => $cmd_key value => $result");
			}
		}
		elsif ($type eq "ce301")
		{
			my $tx_frame = build_tx_frame($type,$cmd_value,$serial);
			$cmd_send++;
                        send $sock,$tx_frame,0;
			select(undef, undef, undef, 0.4);

                        if ($key =~ "uniping")
			{
			}
			else
			{
				my $rx_frame = rcv_buffer($sock,$type,$serial);
				$cmd_recv++;
				my @arr = split('\n', $rx_frame);

	                	my $i = 0;
        	        	foreach my $a (@arr)
                		{
                        	    my @arr2 = split('\(',$a);
                       		    if (($arr2[0]) && ($arr2[1]))
                        	    {
                                	$arr2[1] =~ s/\).*//;
                                	my $z_key = $cmd_value;
                                	$z_key = lc($z_key);
                                	$z_key =~ s/\(.*\)//;
					print "RESULT: KEY $z_key$i FRAME => $arr2[1] \n" if $DEBUG;
					send_to_zbx("$key","$z_key$i","$arr2[1]");
					$i++;
                        	    }
                		}
			}

		}
		elsif ($type eq "ce102m")
		{
			my $tx_frame = build_tx_frame($type,$cmd_value,$serial);
                        $cmd_send++;
                        send $sock,$tx_frame,0;
                        select(undef, undef, undef, 0.4);
		}
		elsif ($type eq "ceb2")
		{
			my $tx_frame = build_tx_frame($type,$cmd_value,$serial);
			$cmd_send++;
                        send $sock,$tx_frame,0;
                        my $rx_frame = rcv_buffer($sock,$type,$serial);
			$cmd_recv++;
                        print "RESULT: KEY $cmd_key FRAME => $rx_frame \n" if $DEBUG;
			send_to_zbx($key,$cmd_key,$rx_frame);

			if ($cmd_key eq "et0pe1")
			{
				$act_sum += $rx_frame;
				$count_sum++;
			}
			if ($cmd_key eq "et0pe2")
			{
				$act_sum += $rx_frame;
				$count_sum++;
			}
			
			if ($count_sum == 2)
			{
				print "RESULT: KEY sum_ener FRAME => $act_sum \n" if $DEBUG;
				send_to_zbx($key,"et0pe0",$act_sum);
			}
		}
		else
		{
			my $tx_frame = build_tx_frame($type,$cmd_value,$serial);
			$cmd_send++;
                        send $sock,$tx_frame,0;
                        my $rx_frame = rcv_buffer($sock,$type,$serial);
			$cmd_recv++;
                        print "RESULT: KEY $cmd_key FRAME => $rx_frame \n" if $DEBUG;
			send_to_zbx($key,$cmd_key,$rx_frame);
		}
	}

	print "closing socket... \n" if $DEBUG;
 	if ($sock)
        {
                shutdown($sock, 2);
                close($sock);
        }

	if (($key =~ "uniping")&&($type eq "ce301"))
	{
		$read_sock->join();
		while (my ($z_key, $z_val) = each(%result))
		{
			print "RESULT: KEY $z_key FRAME => $z_val \n" if $DEBUG;
			send_to_zbx($key,$z_key,$z_val);
			$cmd_recv++;
		}
	}

	if ($type eq "ce102m")
        {

		################
	        ###end of session
	        ################
        	my $cmd_end = "B0" . "\x03";
	        my $bcc = get_bcc($cmd_end);
        	$bcc  = pack "B8", "$bcc";
	        $cmd_end = "\x01" . $cmd_end . $bcc;
        	print "end of session command => $cmd_end\n\n" if $DEBUG;
	        send $sock,$cmd_end,0;
        	select(undef, undef, undef, 0.4);

                $read_sock->join();
                while (my ($z_key, $z_val) = each(%result))
                {
                        print "RESULT: KEY $z_key FRAME => $z_val \n" if $DEBUG;
                        send_to_zbx($key,$z_key,$z_val);
                        $cmd_recv++;
                }
        }

	syslog("info", "closing socket: sent $cmd_send commands, received $cmd_recv packets");

}


closelog();
close(LOCK);
unlink("/tmp/meters.lock");

sub build_tx_frame 
{
	my $type = shift;
	my $cmd = shift;
	my $serial = shift;
	my $phase = shift;
	#########
	###CE102
	#########
	if ($type eq "ce102")
	{
        	if ($phase == "1")
        	{
                	$phase = "\x01";
        	}
	        else
        	{
        	        $phase = "\x00";
       		}

        	#serial to hex
        	my $ser_cmd = substr($serial, -4);
        	$ser_cmd = sprintf("%04x",$ser_cmd);

	        #reverse serial
	        $ser_cmd =  reverse pack 'H*', $ser_cmd;

	        #command
	        my $cmd_ini = "\x48". $ser_cmd ."\x00"."\x00"."\x00"."\x00"."\x00"."\x00"."\xd2". $cmd . $phase . "\x00";

	        #bcc
	        my $ctx = Digest::CRC->new(width=>"8", poly=>0xB5, init=>0x00, xorout=>0x00, refout=>0, refin=>0, cont=>0);
        	$ctx->add($cmd_ini);
	        my $bcc_hex = pack 'H*', $ctx->hexdigest;

	        $cmd_ini = $cmd_ini . $bcc_hex;
	        $cmd_ini = "\xc0" . $cmd_ini . "\xc0";

	        my $tx_hex = unpack('H*', "$cmd_ini");
      		print "Current command (hex) =>  $tx_hex \n" if $DEBUG;

        	return $cmd_ini;
	}
	#########
	###CE301
	#########
	elsif ($type eq "ce301")
	{	
	        my $tx_buffer = "R1" . "\x02" . "$cmd" . "\x03";
        	my $bcc = get_bcc($tx_buffer);
        	$bcc  = pack "B8", "$bcc";

        	$tx_buffer .= $bcc;
        	$tx_buffer = "/?!" .  "\x01" . $tx_buffer;

	        print "Current command => $tx_buffer\n" if $DEBUG;

        	return $tx_buffer;
	}
	#########
        ###CE102M
        #########
        elsif ($type eq "ce102m")
        {
                my $tx_buffer = "R1" . "\x02" . "$cmd" . "\x03";
                my $bcc = get_bcc($tx_buffer);
                $bcc  = pack "B8", "$bcc";

                $tx_buffer .= $bcc;
                $tx_buffer = "\x01" . $tx_buffer;

                print "Current command => $tx_buffer\n" if $DEBUG;

                return $tx_buffer;
        }
	#########
	###CEB2a
	#########
	elsif ($type eq "ceb2") 
	{
		my $pass = "00000";
		my $ser_cmd = substr($serial, -3);
	        my $tx_buffer = "#" . $ser_cmd . $pass . $cmd;
        	my $bin_bcc = get_bcc($tx_buffer);
        	my $hex_bcc = unpack("H*", pack ("B*", $bin_bcc));
        	$hex_bcc = uc($hex_bcc);
        	$tx_buffer = $tx_buffer . $hex_bcc . "\x0d";
		print "Current command => $tx_buffer\n" if $DEBUG;
		return $tx_buffer;
	}
	#########
	###UMG103
	#########
	elsif ($type eq "umg103")
	{
		my $unit_id = "1";
		my $reg_nb   = "2";
	
        	my $func_code = MODBUS_READ_HOLDING_REGISTERS;
        	my $body =  pack("nn", $cmd, $reg_nb);

        	my $func_body = pack("C", $func_code).$body;
        	my $header_id = int(rand 65535);
        	my $tx_header_pr_id = 0;
        	my $tx_header_len = bytes::length($func_body) + 1;
	        my $func_mbap = pack("nnnC", $header_id, $tx_header_pr_id,$tx_header_len, $unit_id);
		pretty_dump('Tx', $func_mbap.$func_body) if $DEBUG;
        	return $func_mbap.$func_body;
	}
}


sub rcv_buffer
{
	my $sock = shift;
	my $type = shift;
	my $serial = shift;

	if ($type eq "ceb2") 
	{
	    eval {
                local $SIG{ALRM} = sub { die "timeout" };
                alarm(2);
		my $ser_cmd = substr($serial, -3);
		my $rx_buffer;
		my $s_recv = recv($sock, $rx_buffer, 32, 0);
                my $rx_frame = $rx_buffer;
                sleep(1);
                
                if (length($rx_buffer) < 8)
                {
                  if (can_read($sock))
                  {
                        $s_recv = recv($sock, $rx_buffer, 32, 0);
                        $rx_frame .= $rx_buffer
                  }
                }

                $rx_frame = substr($rx_frame, 0, -3);
                $rx_frame =~ s/^\~$ser_cmd.//;
		print "Received frame => $rx_frame  Length => " . length($rx_frame) . "\n" if $DEBUG;
		return $rx_frame;

	        alarm(0);
             };

	}
	elsif ($type eq "ce301") 
	{
	        my ($rx_buffer,$rx_frame);
	        #my $s_recv = recv($sock, $rx_buffer, 8, 0);
	        #my $rx_frame = $rx_buffer;

	        #my $rx_hex = unpack('H*', "$rx_frame");

        	eval {
		     local $SIG{ALRM} = sub { die "timeout" };
	             alarm(2);

		     my $s_recv = recv($sock, $rx_buffer, 8, 0);
                     $rx_frame = $rx_buffer;

                     my $rx_hex = unpack('H*', "$rx_frame");


        	     my $last_char = "ne";
        	     if (length($rx_hex) > 2)
       		     {
                	 $last_char = substr($rx_hex,length($rx_hex)-4,2);
		     }
	             #read eoh
        	     while($last_char ne "03")
      		     {
	                #if (can_read($sock))
	                #{
	                     $s_recv = recv($sock, $rx_buffer, 32, 0);
	                     $rx_frame .= $rx_buffer;
	                #}
	                $rx_hex = unpack('H*', "$rx_frame");
	                print "received hex => $rx_hex \n" if $DEBUG;
	                $last_char = substr($rx_hex,length($rx_hex)-4,2);
	             }
	             alarm(0);
	        };

	        print "received complete frame => $rx_frame \n" if $DEBUG;

	        return $rx_frame;
	}
	elsif ($type eq "ce102")
	{
		my ($rx_buffer,$rx_frame);


	        eval {
		    local $SIG{ALRM} = sub { die "timeout" };
	            alarm(2);
			my $s_recv = recv($sock, $rx_buffer, 8, 0);
	                $rx_frame = $rx_buffer;

	                my $rx_hex = unpack('H*', "$rx_frame");
	                my $last_char = "ne";

	                if (length($rx_hex) > 2)
                	{
                        	$last_char = substr($rx_hex,length($rx_hex)-2,2);
                	}

	                while($last_char ne "c0")
	                {
	                        if (can_read($sock))
	                        {
        	                        $s_recv = recv($sock, $rx_buffer, 32, 0);
        	                        $rx_frame .= $rx_buffer
	                        }
        	                $rx_hex = unpack('H*', "$rx_frame");
        	                $last_char = substr($rx_hex,length($rx_hex)-2,2);
	                }
        	    alarm(0);
	        };

	        return $rx_frame;
	}
	elsif ($type eq "umg103")
	{
        	my ($rx_buffer,$rx_frame);
	        my ($rx_unit_id, $rx_bd_fc, $f_body);

        	# 7 bytes head
        	if (can_read($sock))
        	{
        	        my $s_recv = recv($sock, $rx_buffer, 7, 0);
       		}

        	unless($rx_buffer)
        	{
        	        print "Error while receiving data from socket...\n" if $DEBUG;
        	        return undef;  
        	}

	        $rx_frame = $rx_buffer;
	        # decode
	        my ($rx_hd_tr_id, $rx_hd_pr_id, $rx_hd_length, $rx_hd_unit_id) = unpack "nnnC", $rx_frame;
	        if (can_read($sock))
	        {
	                my $s_recv = recv($sock, $rx_buffer, $rx_hd_length+1, 0);
	        }

	        return undef unless($rx_buffer);
	        $rx_frame .= $rx_buffer;

	        pretty_dump('Rx', $rx_frame) if $DEBUG;
	        ($rx_bd_fc, $f_body) = unpack "Ca*", $rx_buffer;

	        # check except
	        if ($rx_bd_fc > 0x80)
	        {
	                # except code
	                my $exp_code = unpack "C", $f_body;
	                print "Exception code (". $exp_code. ") \n" if $DEBUG;
	                return undef;
	        }
		else
        	{
        	        # return
        	        #my ($rx_reg_count, $f_regs) = unpack 'Ca*', $f_body;
        	        #my $r = unpack "f", pack "L", unpack "N", $f_regs;
		
			my ($rx_reg_count, $f_regs) = unpack 'Ca*', $f_body;
		        my $result = unpack "f", pack "L", unpack "N", $f_regs;
		        $result = sprintf("%.3f", $result);


        	        return $result;
        	}

	}

}

#############
### Wait for socket read
#############
sub can_read
{
        my $sock = shift;
        my $hdl_select = "";
        vec($hdl_select, fileno($sock), 1) = 1;
        my $select = select($hdl_select, undef, undef, $timeout);

        if ($select)
        {
                return $select;
        }
        else
        {
                return undef;
        }
}


##################
###$_[0] - packet data
##################
sub get_bcc
{
        my $cmd = shift;
        my $hex = unpack('H*', "$cmd");
        my $hex_sum = 0;

        foreach my $aa (split('(..)',$hex))
        {
                if ($aa)
                {
                        $hex_sum += hex($aa);
                }
        }

        my $bin = sprintf ("%b",$hex_sum);
        $bin = substr($bin, -8);
        while (length($bin) < 8)
        {
                $bin = "0" . $bin;
        }
        return $bin;
}

sub pretty_dump
{
        my $label = shift;
        my $data  = shift;
        my @dump = map {sprintf "%02X", $_ } unpack("C*", $data);
        $dump[0] = "[".$dump[0];
        $dump[5] = $dump[5]."]";
        print $label."\n";
        for (@dump)
        {
                print $_." ";
        }
        print "\n";
}


sub get_ce102_data
{
        my $rx_frame = shift;
        my $len = shift;

        my $rx_hex = unpack('H*', "$rx_frame");
        print "received frame: $rx_hex \n" if $DEBUG;

        $rx_hex = unpack 'H*', reverse pack 'H*', $rx_hex;

        if (($rx_hex =~ /^c0/) && ($rx_hex =~ /c0$/))
        {
                $rx_hex =~ s/^c0..//;
                $rx_hex = substr($rx_hex, 0, $len);
                my $dec = sprintf("%d", hex($rx_hex));
                print "DATA HEX: $rx_hex \n" if $DEBUG;
                print "DATA RESULT: $dec \n" if $DEBUG;
                #$rx_hex = unpack 'H*', reverse pack 'H*', $rx_hex;
                return $dec;
        }
}

sub send_to_zbx
{
        my $key = shift;
        my $cmd_key = shift;
        my $cmd_val = shift;
        system("$zabbix_sender -z \"$z_ip\" -s \"$key\" -k \"$cmd_key\" -o \"$cmd_val\" > /dev/null 2>&1");
}

sub read_data {
    my $curr_sock = shift;
    my $flags = fcntl($curr_sock, F_SETFL, 0) or die "Can't set flags for the socket: $!\n";
    while (<$curr_sock>)
    {
        my @response = split ' ', $_;
        foreach my $rcv (@response)
        {
		$rcv =~ s/^\(//;
                my ($name, $val) = split('\(',$rcv,2);
                if ((defined $name) && (defined $val))
                {
			$name = lc($name);
                        $val =~ s/\).*//g;

			#################
			##create result hash
			#################
			while (my ($k, $v) = each(%{$commands{"commands_ce301"}}))
			{
				$v = lc($v);
				$v =~ s/\(.*\)//;
				if ($name =~ $v)
				{
					$name = $v;
				}
			}

                        my $z_key = $name;

			if (grep {/$z_key\d+/} keys %result)
                	{

				my @numbers = ();
                        	foreach my $a (grep {/$z_key\d+/} keys %result)
                        	{
			                my $b = substr($a, -1);
                        	        push (@numbers, $b);
                        	}
			        @numbers = sort {$a <=> $b} @numbers;
             			my $new_num = $numbers[-1];
			        $new_num++;
                        	$result{"$z_key" . "$new_num"} = $val;

 			}
			else
			{
				$result{"$z_key" . "0"} = $val;
			}

		}
	}
    }
}
