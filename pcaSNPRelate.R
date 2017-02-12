if(!require(gdsfmt)) {
  biocLite("gdsfmt"); require(gdsfmt)}

if(!require(SNPRelate)) {
  biocLite("SNPRelate"); require(SNPRelate)}

# vcf.fn <- "/mnt/zgmvol/_forge/zsibio/Anastasiia/chr22.vcf"
# snpgdsVCF2GDS(vcf.fn, "/mnt/zgmvol/_forge/zsibio/Anastasiia/chr22", method = "biallelic.only")

# genofile <- snpgdsOpen(gts.fn)

GetPcaDF <- function(gts.fn, panel.fn, output.fn, maf.set, missing.rate, variance.th.set = 0.7, ld.th.set = 0){
#  genofile <- snpgdsOpen(gts.fn)
  
for (ld.th in ld.th.set){  
 for (maf in maf.set){
  snpset.id = NULL
  if (ld.th != 0){
    snpset <- snpgdsLDpruning(genofile, snp.id=snpset.id, autosome.only  = FALSE, ld.threshold = ld.th, maf = maf)
    snpset.id <- unlist(snpset)
  }
  
  # ------- 
  # PCA
  pca <- snpgdsPCA(genofile, snp.id=snpset.id,  bayesian = TRUE, eigen.cnt=0, num.thread=8, autosome.only  = FALSE, missing.rate = missing.rate)
  
  # ------- 
  # Calculate total variance to get optimal Nr of PCs
  # eigenvalues <- na.omit(pca$eigenval)
  # variance <- eigenvalues / sum(eigenvalues)
  variance <- pca$varprop
  cum.variance <- cumsum(variance)
 for (variance.th in variance.th.set){
  n.pc <- length(which((cum.variance <= variance.th) == TRUE))
  
  # -------
  # Get Region form panel file
  panel <- read.table(panel.fn, header = T)
  pop.df <- panel[, c(1,3)]
  colnames(pop.df) <- c("sampleId", "Region")
  
  # ------- 
  # Create pcaDF
  pca.df <- as.data.frame(pca$eigenvect)
  pc.vector <- rep("PC", ncol(pca.df))
  col.names <- paste0(pc.vector, 1:ncol(pca.df))
  colnames(pca.df) <- col.names
  pca.df <- data.frame(SampleId = pca$sample.id, 
                      Region = pop.df$Region,
                      pca.df)
  pca.df <- pca.df[, 1:(n.pc + 2)]
  cur.output.fn <- paste(gts.fn, ld.th, maf, missing.rate, variance.th, ".csv", sep="_")
  write.csv2(pca.df, cur.output.fn, quote = FALSE, row.names = FALSE)
  # return (pca.df)
}
}
}
}

gts.fn <- "/mnt/zgmvol/_forge/zsibio/Anastasiia/chr22"
panel.fn <- "/mnt/zgmvol/_forge/zsibio/Anastasiia/ALL.panel"
# gts.fn <- "/home/anastasiia/1000genomes/chr22"
# panel.fn <- "/home/anastasiia/1000genomes/ALL.panel"
ld.th.set = 0.2 # c(0.2, 0.5, 0.7)
maf.set = 0.005 #c(0.005, 0.05, 0.5)
missing.rate = 0.0
variance.th.set = 0.1 #c(0.25, 0.5, 0.7)
# output.fn <- paste(gts.fn, )

genofile <- snpgdsOpen(gts.fn)

# output.fn <- paste(gts.fn, ld.th, maf, missing.rate, variance.th, ".csv", sep="_")
	do.call(GetPcaDF, list(gts.fn, panel.fn, "", maf.set, missing.rate, variance.th.set, ld.th.set))

