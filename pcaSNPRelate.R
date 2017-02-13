if(!require(gdsfmt)) {
  biocLite("gdsfmt"); require(gdsfmt)}

if(!require(SNPRelate)) {
  biocLite("SNPRelate"); require(SNPRelate)}

if(!require(dplyr)) {
  install.packages("dplyr"); require(dplyr)}

# vcf.fn <- "/mnt/zgmvol/_forge/zsibio/Anastasiia/chr22.vcf"
# snpgdsVCF2GDS(vcf.fn, "/mnt/zgmvol/_forge/zsibio/Anastasiia/chr22", method = "biallelic.only")

# genofile <- snpgdsOpen(gts.fn)

GetPcaDF <- function(gts.fn, panel.fn, output.fn, snpset.idx, maf.set, missing.rate, variance.th.set = 0.7, ld.th.set = 0){
#  genofile <- snpgdsOpen(gts.fn)
  
  snpset.id <- snpset.idx
  for (ld.th in ld.th.set){  
    for (maf in maf.set){
        if (ld.th != 0){
          snpset <- snpgdsLDpruning(genofile, snp.id = snpset.id, autosome.only  = FALSE, ld.threshold = ld.th, maf = maf)
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

variant.fn <- "~/1000genomes/freq_chr22-sample.csv"
gts.fn <- "~/1000genomes/chr22"
gts.fn <- "/mnt/zgmvol/_forge/zsibio/Anastasiia/chr22"
variant.fn <- "/mnt/zgmvol/_forge/zsibio/Anastasiia/freq_chr22.csv"
panel.fn <- "/mnt/zgmvol/_forge/zsibio/Anastasiia/ALL.panel"
# gts.fn <- "/home/anastasiia/1000genomes/chr22"
# panel.fn <- "/home/anastasiia/1000genomes/ALL.panel"
ld.th.set = 0.2 # c(0.2, 0.5, 0.7)
# maf.set = 0.005 #c(0.005, 0.05, 0.5)
missing.rate = 0.0
variance.th.set = 0.1 #c(0.25, 0.5, 0.7)
inf.freq <- 0.05
sup.freq <- 0.5
output.fn <- NULL

genofile <- snpgdsOpen(gts.fn)
snp.positions.set <- read.gdsn(index.gdsn(genofile, "snp.position"))
snp.chr.set <- read.gdsn(index.gdsn(genofile, "snp.chromosome"))
snp.idx.set <- read.gdsn(index.gdsn(genofile, "snp.id"))
snp.id.set <- paste(snp.chr.set, snp.positions.set, sep = ":")
snp.id.idx.df <- cbind(as.data.frame(snp.id.set), as.data.frame(snp.idx.set))

variants <- read.csv(variant.fn, sep = ",")[, c("contigName", "start", "frequency")]
variants <- variants %>% mutate(snpIdx = paste(contigName, start, sep = ":")) %>% select(snpIdx, frequency)

selected.snp.idx <- variants[variants$frequency >= inf.freq & variants$frequency <= sup.freq, "snpIdx"]
snp.id.set <- snp.id.idx.df[snp.id.idx.df$snp.id.set %in% selected.snp.idx, "snp.idx.set"]

# output.fn <- paste(gts.fn, ld.th, maf, missing.rate, variance.th, ".csv", sep="_")
do.call(GetPcaDF, list(gts.fn, panel.fn, output.fn, snp.id.set, NaN, missing.rate, variance.th.set, ld.th.set))

ggplot(pca.df, aes(x = PC1, y = PC2, color = Region)) +
       geom_point() 

corr <- snpgdsPCACorr(pca, genofile, snp.id=snpset.id)$snpcorr
require(MASS)
chisq.test(corr)
       