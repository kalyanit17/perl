#!/usr/bin/perl

use threads ;
use threads::shared ;
use Getopt::Long ;
use File::Find ;
use File::stat ;
use Cwd qw(getcwd abs_path);
use File::Temp qw(tempfile);

my $dirPath ;
my $filterExpr ;
my $rblockSize ;
my $readSize 		;
my $wblockSize ;
my $writeSize		;
my $numThreads ;
my $numIterations ;
my (@ALLFILES,$threadExitCond,$counter) :shared ;

my $READ_SIZE  	  = 4194304 ;
my $READ_BLKSIZE    = 262144  ;
my $WRITE_SIZE 	  = 4194304 ;
my $WRITE_BLKSIZE   = 262144  ;
my $DEFAULT_THREADS = 10      ;
my $NUM_ITERATIONS  = 5       ;

GetOptions (
	'p|path=s' 		=> \$dirPath,
	'f|filter=s' 	=> \$filterExpr,
	'rsize=s'		=> \$readSize,
	'wsize=s'	 	=> \$writeSize,
	'rblock=s'     => \$rblockSize,
	'wblock=s'		=> \$wblockSize,
	'iters=i'		=> \$numIterations,
	't|threads=i'  => \$numThreads,
) or die "Error processing cmd options :: $! ";

if (!defined($dirPath)) {
	$dirPath = getcwd();
}
else {
	if ( ! -d $dirPath ) {
		die "Dir [ $dirPath ] doesn't exist \n";
	}
	$dirPath = abs_path($dirPath) ;
}

if (!defined($rsize)) {
	$rsize = $READ_SIZE; # 4*1024 bytes read
}

if (!defined($wsize)) {
	$wsize = $WRITE_SIZE; # 4*1024 bytes write
}

if (!defined($numThreads)) {
	$numThreads = 10;
}

if (!defined($rblockSize)) {
	$rblockSize = $READ_BLKSIZE ;
}

if (!defined($wblockSize)) {
	$wblockSize = $WRITE_BLKSIZE ;
}

if ($rblockSize !~ /^\d+k?$/i ) {
	die "Invalid input read block size [ $rblockSize ] \n";
}
else {
	if ( $rblockSize =~ /^(\d+)k$/i ) {
		$rblockSize = $1*1024;
	}
}

if ($wblockSize !~ /^\d+k?$/i ) {
   die "Invalid input write block size [ $wblockSize ] \n";
}
else {
	if ( $wblockSize =~ /^(\d+)k$/i ) {
		$wblockSize = $1*1024;
	}
}

if (!defined($readSize)) {
	$readSize = $READ_SIZE ;	
}
else {
	if ( $readSize !~ /^(\d+)(k?)$/i ) {
		die "Invalid input read size [ $readSize ] \n";
	}

	if ( defined $2 ) {
		$readSize = $1*1024 ;
	}
}

if (!defined($writeSize)) {
	$writeSize = $WRITE_SIZE ;	
}
else {
	if ( $writeSize !~ /^\d+(k?)$/i ) {
		die "Invalid output write size [ $writeSize ] \n";
	}

	if ( defined $2 ) {
		$writeSize = $1*1024 ; 
	}
}

if (!defined($numIterations)) {
   $numIterations = $NUM_ITERATIONS;
}

$counter = 0; 

$threadExitCond = $numIterations ;

my ($outputFH, $outputFile) = tempfile();

my $analyzeDirPath = threads->create('analyzeDir',$outputFH,$dirPath,);

my $totalFiles = $analyzeDirPath->join();


seek($outputFH,0,0);

my $populateFileQueue = threads->create('addFilesQueue',$outputFH);

open DEVNULL,'>',File::Spec->devnull or die "File open on /dev/null failed : $! ";

consumeFiles($numThreads);

$populateFileQueue->join();

#while(my $line = <$outputFH> ) {
#	chomp($line);
#	print "line :: $line \n";
#}

sub analyzeDir() {
	my ($outputhandle, $dirpath) = @_;

	my ($file, $fsize, $numfiles) ;

	$numfiles = 0;

	find(sub { 
					if ( -f $_ && $_ !~ /^\.\.?$/ ) {
						++$numfiles ;
						$file = $File::Find::name ;
						$fsize = stat($file)->size ;
						print ${outputhandle} "$file $fsize \n" ;
					} 
				}, $dirpath);
	return $numfiles;
}

sub addFilesQueue() {
	my ($fh ) = @_ ;
	
	seek($fh,0,0);
	while (my $file = <$fh> ) {
		chomp($file);
		push @ALLFILES, $file;
	}
}

sub consumeFiles() {
	my ($thread_count) = @_; 

	my @ths ;
	
	for ( my $i = 1; $i <= $thread_count; $i++ ) {
		my $th = threads->create('processFile',$readSize, $writeSize);
		push @ths, $th;	
	}

	foreach ( @ths ) {
		$_->join();
	}
}

sub processFile() {

	while ( @ALLFILES > 0 || $threadExitCond > 0 ) {
		my $file = shift @ALLFILES ;
		#print " $threadExitCond \n" ;
		if ( $threadExitCond > 1 ) {
			push @ALLFILES, $file ;
		}

		if (defined($file)) {
			#print "Processing file :: $file \n ";
			readNwrite(split /\s+/, $file);
		}

		lock $counter;
		++$counter;

		if ( $counter == $totalFiles ) {
			$counter = 0;
			$threadExitCond--;
		}
	}
}

sub readNwrite() {
	my ($fname, $fsize) = @_ ;	
	my $start_offset ;

	print "Processing file :: $fname file size :: $fsize \n";
	if ($counter%2 == 0 ) {
		if ( $fsize < $readSize ) {
			print "Reading the entire file [ $fname ]  \n";
			open my $rfh, '+<:encoding(utf8',$fname or die "File open [ $fname ] failed : $!" ;
			binmode($rfh);
			while(<$rfh>) {
				print DEVNULL $_;
			}
			close $rfh;
		}
		else {
			$start_offset = int rand($fsize-$readSize);
			print "Reading file [ $fname ] from offset [ $start_offset ] read size [ $readSize ] \n";
			open my $rfh, '+<:encoding(utf8)',$fname or die "File open [ $fname ]  failed : $!";
			binmode($rfh);
			seek($fh,$start_offset,0);
			my $line;
			my $read=0;
			while(read($rfh,$line,1024) && ( $read<$readSize )) {
				print DEVNULL $line;
				$read = $read+1024;
			}
			close $rfh;		
		}
	}
}

