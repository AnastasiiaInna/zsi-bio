if(!require(gdsfmt)) {
  biocLite("gdsfmt"); require(gdsfmt)}

if(!require(SNPRelate)) {
  biocLite("SNPRelate"); require(SNPRelate)}

vcf.fn <- "/mnt/zgmvol/_forge/zsibio/Anastasiia/chr22.vcf"
snpgdsVCF2GDS(vcf.fn, "/mnt/zgmvol/_forge/zsibio/Anastasiia/chr22", method = "biallelic.only")

GetPcaDF <- function(gts.fn, panel.fn, maf, missing.rate, variance.th = 0.7, ld.th = 0){
  genofile <- snpgdsOpen(gts.fn)
  
  snpset.id = NULL
  if (ld.th != 0){
    snpset <- snpgdsLDpruning(genofile, autosome.only  = FALSE, maf = 0.005, ld.threshold = ld.th)
    snpset.id <- unlist(snpset)
  }
  
  # ------- 
  # PCA
  pca <- snpgdsPCA(genofile, snp.id=snpset.id, num.thread=2, autosome.only  = FALSE, maf = maf, missing.rate = missing.rate)
  
  # ------- 
  # Calculate total variance to get optimal Nr of PCs
  eigenvalues <- na.omit(pca$eigenval)
  variance <- eigenvalues / sum(eigenvalues)
  cum.variance <- cumsum(variance)
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
  pca.df <- data.frame(sampleId = pca$sample.id, 
                      Region = pop.df$Region,
                      pca.df)
  pca.df <- pca.df[, 1:(n.pc + 2)]
  write.csv2(pca.df, "/mnt/zgmvol/_forge/zsibio/Anastasiia/pcaDF.cssv", quote = FALSE, row.names = FALSE)
  return (pca.df)
}

gts.fn <- "/mnt/zgmvol/_forge/zsibio/Anastasiia/chr22"
panel.fn <- "/mnt/zgmvol/_forge/zsibio/Anastasiia/ALL.panel"
# gts.fn <- "/home/anastasiia/1000genomes/chr22"
# panel.fn <- "/home/anastasiia/1000genomes/ALL.panel"
ld.th = 0.2
maf = 0.005
missing.rate = 0.0
variance.th = 0.7

do.call(GetPcaDF, list(gts.fn, panel.fn, maf, missing.rate, variance.th))
