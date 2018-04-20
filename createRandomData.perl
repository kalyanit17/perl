#!/usr/bin/perl

use Getopt::Long qw(GetOptions); 
use Cwd qw(getcwd) ;
use File::Path qw(make_path remove_tree);
use POSIX ;
use threads ;
use threads::shared ;

my $dirPath ;
my $numFiles ;
my $fileSize ;
my $blockSize ; 
my $numBlocks ;
my $numThreads ;
my $filePrefix ;
my $order ;
my $depth ;
my $dirPrefix ;
my $maxDirs ;
my ($threadExitCond, @ALLFILES) :shared ;


my $NUMFILES   = 100 ;
my	$FILESIZE   = 64 ; # 64 * 256k = 16MB
my	$NUMBLOCKS  = 64 ;
my	$BLOCKSIZE  = '256k' ;
my	$NUMTHREADS = 10 ;
my	$FILEPREFIX = 'file';
my	$DIRPREFIX  = 'DIR' ;
my	$ORDER      = 1 ;
my	$DEPTH      = 1 ;

sub Help() {
	print "################################################################################## \n";
	print "USAGE :: $0 -p <path> -c <numfiles> -s <filesize> -b [<blockSize>]        			\n";
	print " -p|--path									Dir to create files								      \n";
	print " -c|--count 								No. of files to create 							      \n";
	print " -s|--size									Size of a file 									      \n";
	print " -t|--threads                      No. threads to spawn                            \n";
	print "--bs 										BlockSize to use for dd command file creation   \n";
	print "--fprefix									Filename prefix for all files 						\n";
	print "--dprefix									Dirname prefix for all dirs							\n";
	print "-o|--order 								Order of the dir tree									\n";
	print "-d|--depth									Height of the dir tree									\n";
	print "################################################################################## \n";
	exit(1);
}

GetOptions(	'p|path=s' 		=> \$dirPath,
				'c|count=i', 	=> \$numFiles,
				's|size=s',		=> \$fileSize,
				'bs:s'			=> \$blockSize,
				't|threads=i'    => \$numThreads,
				'fprefix'		=> \$filePrefix,
				'dprefix'		=> \$dirPrefix,
				'o|order=i'		=> \$order,
				'd|depth=i'		=> \$depth,
				'h|help'			=> sub { Help()  } ) or die "Error in command line arguments \n" ;

if (defined($dirPath)) {
	if (! -d $dirPath ) {
		make_path($dirPath,{'verbose' => 1 }) or die "Dir [ $dirPath ] create failed : $! \n";
	}
}
else {
	$dirPath = getcwd();
	print "Not input dirPath provided ... Assuing cwd [ $dirPath ] ..\n" ;
}

if (!defined($dirPrefix)) {
	$dirPrefix = $DIRPREFIX;
}

if (defined($numFiles) ) {
	if ( $numFiles !~ /^\d+$/ ) {
		die "Input numFiles [ $numFiles ] not a integer ..\n" ;
	}
}
else {
	$numFiles = $NUMFILES;
}

$threadExitCond = $numFiles ;

if (!defined($blockSize) ) {
	$blockSize = $BLOCKSIZE ; # Default block size of 256k		
}
elsif ( $blockSize =~ /^(\d+)(k|kb)$/i ) {
	$blockSize = $1 ;
}
else {
	die "Invalid blocksize [ $blockSize ] , should be in kb's only ...\n"
}

if (defined($numThreads)) {
	if ($numThreads !~ /^\d+$/ ) {
		die "No. of threads [ $numThreads ] is not an integer ... \n";	
	}
}
else {
	$numThreads = $NUMTHREADS;
}

if (defined($fileSize)) {
	if ( $fileSize =~ /^(\d+)(k|m|g|kb|mb|gb)$/i ) {
		if ( $1 == 0 ) {
			die "fileSize [ $fileSize ] should be non-zero ...\n";
		}		

		if ( lc($2) eq "k" or lc($2) eq "kb" ) {
			if ( $1 < $blockSize ) {
				$blockSize = "1";
				$numBlocks = $1 ;
			}
			else {
				$numBlocks = ceil($1/$blockSize) ;	
			}
			print " Determined blocksize as [ $blockSize ] and numBlocks as [ $numBlocks ] \n";		
		}
		elsif ( lc($2) eq "m" or lc($2) eq "mb" ) { 
			$numBlocks = ceil(($1*1024)/$blockSize) ;
			print " Determined blocksize as [ $blockSize ] and numBlocks as [ $numBlocks ] \n";			
		}
		elsif ( lc($2) eq "g" or lc($2) eq "gb" ) {
			$numBlocks = ceil(($1*1024*1024)/$blockSize) ;
			print " Determined blocksize as [ $blockSize ] and numBlocks as [ $numBlocks ] \n";      
		}
	}	
	else {
		die "Unrecognized fileSize [ $fileSize ] \n";
	}
	$blockSize = $blockSize."k" ;
}
else {
	$fileSize = $FILESIZE ; # Default file size 16MB files 
	$numBlocks = $NUMBLOCKS;
}

if (!defined($filePrefix)) {
	$filePrefix = $FILEPREFIX; # Defualt file prefix
}

if (!defined($order)) {
	$order = $ORDER;
}

if (!defined($depth)) {
	$depth = $DEPTH;
}

if ( $depth == 1 && $order == 1 ) {
	$maxDirs = $depth*$order;
}
else {
	$maxDirs = $order**$depth-1 ;
}

createDirTree($depth,$order,$maxDirs);

my $fileCreateThread = threads->create('createFilesDirTree',$dirPath,$depth,$order,$numFiles,$maxDirs,$dirPrefix,$filePrefix);

$fileCreateThread->join();

consumeFilesDirTree($blockSize,$numBlocks);

#foreach my $file ( @ALLFILES ) {
#	print "FILE :: $file \n" ;
#}

sub createDirTree() {
	my ($d,$o,$maxDirs) = @_;

	print "maxdirs :: $maxDirs \n";
	if ( $d == 1 ) {
		my $createDir;
		for (my $j=1; $j<=$o; $j++ ) {
			$createDir = File::Spec->catdir($dirPath,$dirPrefix."$j");
			make_path($createDir) or die "Dir [ $createDir ] creation failed $! ..\n";
		}
		
	}
	else {
		my $th = threads->create('createDirSubTree',$dirPath,0,$o,$maxDirs) ;
		if(!defined($th)) {
			die "Thread create failed for dir tree create  $! \n";
		}

		$th->join();
	}
	print "Dir tree creation complete \n";
}

sub createDirSubTree() {
	my ($dir,$parent,$order,$maxdirs) = @_ ;
	my $createDir ;
	my $index;	

	foreach my $f (1..$order) {
		$index = $order*$parent+$f ;
		if($index > $maxdirs ) {
			return ;
		}
		$createDir = File::Spec->catfile($dir,$dirPrefix.$index);
		make_path($createDir) or die "Dir [ $createDir ] failed :$! \n";
		createDirSubTree($createDir,$index,$order,$maxdirs);
		# system('dd if=/dev/urandom of=$filePath bs=$blockSize count=$numBlocks') or warn "File creation failed for [ $filePath ] ..";
	}
}

sub consumeFilesDirTree() {
	my ($blk_size,$num_blocks) = @_;
	my @ths ;
	print "numThreads :: $numThreads \n" ;
	for (my $j=1; $j <= $numThreads; $j++) {
		my $th = threads->create('createFile',$threadExitCond,\@ALLFILES,$blk_size,$num_blocks);
		sleep 3;
		print "TID :: ",$th->tid()," \n";
		push @ths, $th;		
	}

	foreach ( @ths ) {
		$_->join();
	}

}

sub createFilesDirTree() {
	my ($basedir,$depth, $order, $numFiles, $maxDirs, $d_prefix, $f_prefix) = @_ ;

	my $dirCounter = 1;
	my $currFilePath ;

	for (my $i=1; $i<=$numFiles; $i++) {
		$currFilePath = File::Spec->catfile(File::Spec->catdir($basedir,populateDirPath($dirCounter,$order, $d_prefix)),$f_prefix.$i);
		push @ALLFILES, $currFilePath;		
		if ( $dirCounter == $maxDirs ) {
			$dirCounter = 1;
			next ;
		}
		$dirCounter++;
	}
}

sub populateDirPath() {
	my $temp = "";
	my ($dirIndex,$order,$prefix)  = @_;

	my $p;

	while ( $dirIndex > 0 ) {
		$p = floor(($dirIndex-1)/$order) ;
		$temp = File::Spec->catdir($prefix.$dirIndex,$temp);
		$dirIndex = $p;
	}
	return $temp;
}

sub createFile() {
	share (my $fileArrayRef);
	my ($exitCond,$fileArrayRef,$blk_size,$num_blocks) = @_ ;
	my $filepath ;
	while ( @$fileArrayRef !=0 && $exitCond > 0 ) {
		$filepath = shift @$fileArrayRef;
		$exitCond--;
		my $tid = threads->self()->tid();
		print "Creating file :: [ $filepath ] Thread id :: [ $tid ]\n";
		system("dd if=/dev/urandom of=$filepath bs=$blk_size count=$num_blocks") == 0 or warn "File creation failed for [ $filepath ] ..";
	}
}
