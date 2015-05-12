if [ ! -d "~/ivy2custom" ]; then
  echo "downloading ivy cache"
  wget  https://www.dropbox.com/s/ypfu32hzb17sbmz/custom-ivy2.tar.gz?dl=0 -O ~/custom-ivy2.tar.gz
  echo "uncompressing ivy cache"
  tar -zxvf ~/custom-ivy2.tar.gz
  echo "cleaning up"
  rm "~/custom-ivy2.tar.gz"
  echo "done!"
fi