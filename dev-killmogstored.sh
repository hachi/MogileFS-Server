#!/bin/sh

ps afx | grep mogstore | perl -npe 's/\spts.+//' | xargs kill
