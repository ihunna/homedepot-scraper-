import requests,json,os,uuid,time,random,string,csv, http.client
from utils import Utils

from requests.exceptions import ProxyError, ConnectionError, Timeout
http.client._MAXHEADERS = 1000
from urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)
from datetime import datetime,timedelta,timezone
import concurrent.futures


"""
Creating a class for Homedepot
"""

class HomeDepot():
    def __init__(self,proxies = None):
        self.proxies = Utils.load_proxies() if proxies is None else [proxies]
        self.proxy_cert = False
        self.headers = {
            'referer': 'https://www.homedepot.com/',
            'x-debug': 'false',
            'x-hd-dc': 'origin',
            'origin': 'https://www.homedepot.com',
            'sec-fetch-dest': 'empty',
            'sec-fetch-site': 'same-site',
            'x-api-cookies': '{}',
            'accept-language': 'en-GB,en;q=0.9',
            'accept': '*/*',
            'content-type': 'application/json',
            'x-experience-name': 'hd-home',
            'sec-fetch-mode': 'cors',
        }

        self.root_dir = os.path.dirname(__file__)
        self.name = 'homedepot'

    def load_categories(self):
        try:
            cats,cats_file = [],os.path.join(self.root_dir,'categories.json')
            with open(cats_file,'r',encoding='utf-8') as f:
                cats = json.load(f)['categories']
            return True,cats
        except Exception as error:
            return False,error
        
    def generate_sensor_data(self,type="sensor_data"):
        if type == "sensor_data":
            return '4,i,' +''.join(random.choices(string.ascii_letters + string.digits, k=1242)) + f'==${random.randint(1,20)},{random.randint(1,20)},{random.randint(1,20)}$$'

        elif type == "trace_id":
            return ''.join(random.choices(string.digits + string.digits+ string.ascii_letters.lower() + string.digits, k=32))
        
        elif type=="mcvisId":
            return ''.join(random.choices(string.digits + string.digits, k=38))
        
        elif type == "x-nativeapp":
            return ''.join(random.choices(string.ascii_letters + string.digits, k=16))
    
        
    def format_data(self,store,sku,delay,timeout,data={},multiple=False):
        try:
            if not multiple and len(data.keys()) == 0:
                success,data = self.get_product_details(store,delay,timeout,sku=sku)
                if not success: return False, data
                data = data['data']['product']

            name = data['identifiers']['productLabel']
            brand = data['identifiers']['brandName']
            url = f"https://homedepot.com/{data['identifiers']['canonicalUrl']}"

            reviews = data['reviews']['ratingsReviews']['totalReviews']
            rating = data['reviews']['ratingsReviews']['averageRating']
            
            model = data['identifiers'].get('modelNumber',None)
            retailer = "HomeDepot"
            storesku = data['identifiers'].get('storeSkuNumber',None)
            omsid = data['identifiers'].get('parentId',None)
            store_name = store['store_name']
            store_id = store['store_id']
            store_location = f"{store['address']}, {store['city']}, {store['state']} {store['zipcode']}"


            inventory = data['fulfillment']['fulfillmentOptions']
            total = 0


            if inventory is not None:
                a_q = [loc['locations'] for i in inventory for loc in i['services']  if loc['type'] == 'bopis']

                if len(a_q) < 1:
                    a_q = [loc['locations'] for i in inventory for loc in i['services']  if loc['type'] == 'express delivery']
                
                total = sum([loc['inventory']['quantity'] for i in a_q for loc in i if loc['locationId'] == store['store_id']])

            image_url = data['media'].get('images',None)
            mainImageurl = str(image_url[0].get('url')).replace('<SIZE>','400') if image_url is not None else image_url
            
            result = {
                "name":name,
                "brand":brand,
                "url":url,
                "mainImageurl":mainImageurl,
                "sku":sku,
                "reviews":reviews,
                "rating":rating,
                "model":model,
                "retailer":retailer,
                "storesku":sku,
                "omsid":omsid,
                "store_name":store_name,
                "store_id":store_id,
                "store_location":store_location,
                "inventory":total
            }
            
            # print(f"from store {store['store_id']} | item {sku}")
            return True,result
        except Exception as error:
            return False,error

    def get_product_details(self,store,delay,timeout,sku='',limit:int=20,offset:int=0,cat_id='*',cat_url='',verify=True,multiple=False,retries=Utils.get_retries_count()):
        success,result = False,{}
        try:
            time.sleep(delay)
            proxies = random.choice(self.proxies) if self.proxies is not None and len(self.proxies) > 0 else None
            if verify:verify = self.proxy_cert

            device_id,headers = str(uuid.uuid4()).upper(),self.headers
            headers.update({
                'x-parent-trace-id': f'{self.generate_sensor_data(type="trace_id")}/1352142427325640755',
                'x-cloud-trace-context': f'{self.generate_sensor_data(type="trace_id")}/24;o=1',
                'user-agent': f'THDConsumer/7.55.3.1 (iPhone;iOS 18.6.2) DID:{device_id}',
                'x-nativeapp': f'{self.generate_sensor_data(type="x-nativeapp")}',
                'deviceid': device_id,  
            })
            
            if not multiple:

                params = {
                    'opname': 'clientOnlyProduct',
                }

                json_data = {
                    'operationName': 'clientOnlyProduct',
                    'variables': {
                        'skipPaintDetails': True,
                        'skipSpecificationGroup': False,
                        'skipFavoriteCount': False,
                        'skipInstallServices': True,
                        'skipKPF': False,
                        'skipSubscribeAndSave': False,
                        'itemId': f'{sku}',
                        'storeId': f'{store["store_id"]}',
                        'zipCode': f'{store["zipcode"]}',
                    },
                    'query': 'query clientOnlyProduct($itemId: String!, $loyaltyMembershipInput: LoyaltyMembershipInput, $dataSource: String, $storeId: String, $configId: String, $skipPaintDetails: Boolean = true, $skipSpecificationGroup: Boolean = false, $zipCode: String, $quantity: Int, $skipFavoriteCount: Boolean = false, $skipInstallServices: Boolean = true, $skipKPF: Boolean = false, $skipSubscribeAndSave: Boolean = false) {\n  product(\n    itemId: $itemId\n    loyaltyMembershipInput: $loyaltyMembershipInput\n    dataSource: $dataSource\n  ) {\n    dataSources\n    info {\n      bathRenovation\n      replacementOMSID\n      label\n      hasVisuallySimilar\n      hidePrice\n      ecoRebate\n      quantityLimit\n      categoryHierarchy\n      sskMin\n      sskMax\n      unitOfMeasureCoverage\n      wasMaxPriceRange\n      wasMinPriceRange\n      productSubType {\n        name\n        link\n        __typename\n      }\n      prop65Warning\n      prop65Message\n      returnable\n      hasSubscription\n      isBuryProduct\n      isSponsored\n      isGenericProduct\n      isLiveGoodsProduct\n      sponsoredBeacon {\n        onClickBeacon\n        onViewBeacon\n        onClickBeacons\n        onViewBeacons\n        __typename\n      }\n      sponsoredMetadata {\n        campaignId\n        placementId\n        slotId\n        sponsoredId\n        trackSource\n        __typename\n      }\n      globalCustomConfigurator {\n        customExperience\n        __typename\n      }\n      samplesAvailable\n      customerSignal {\n        previouslyPurchased\n        __typename\n      }\n      productDepartmentId\n      productDepartment\n      augmentedReality\n      swatches {\n        isSelected\n        itemId\n        label\n        swatchImgUrl\n        url\n        value\n        __typename\n      }\n      totalNumberOfOptions\n      paintBrand\n      dotComColorEligible\n      fiscalYear\n      classNumber\n      recommendationFlags {\n        visualNavigation\n        packages\n        pipCollections\n        ACC\n        collections\n        frequentlyBoughtTogether\n        bundles\n        __typename\n      }\n      pipCalculator {\n        coverageUnits\n        display\n        publisher\n        toggle\n        defaultAdditionalCoverage\n        additionalCoveragePercentage\n        __typename\n      }\n      __typename\n    }\n    itemId\n    taxonomy {\n      breadCrumbs {\n        browseUrl\n        creativeIconUrl\n        deselectUrl\n        dimensionName\n        label\n        refinementKey\n        url\n        __typename\n      }\n      brandLinkUrl\n      __typename\n    }\n    availabilityType {\n      discontinued\n      type\n      status\n      buyable\n      __typename\n    }\n    identifiers {\n      canonicalUrl\n      itemId\n      brandName\n      productLabel\n      productType\n      skuClassification\n      modelNumber\n      storeSkuNumber\n      specialOrderSku\n      toolRentalSkuNumber\n      rentalCategory\n      rentalSubCategory\n      upc\n      upcGtin13\n      brandMainPageUrl\n      parentId\n      isSuperSku\n      roomVOEnabled\n      __typename\n    }\n    media {\n      images {\n        url\n        type\n        subType\n        sizes\n        hotspots {\n          coordinate {\n            xCoordinate\n            yCoordinate\n            __typename\n          }\n          omsIDs\n          __typename\n        }\n        altText\n        __typename\n      }\n      video {\n        url\n        videoStill\n        link {\n          text\n          url\n          __typename\n        }\n        title\n        type\n        videoId\n        thumbnail\n        longDescription\n        shortDescription\n        uploadDate\n        dateModified\n        __typename\n      }\n      threeSixty {\n        id\n        url\n        __typename\n      }\n      augmentedRealityLink {\n        usdz\n        image\n        __typename\n      }\n      richContent {\n        content\n        displayMode\n        salsifyRichContent\n        __typename\n      }\n      __typename\n    }\n    badges(storeId: $storeId) {\n      label\n      name\n      color\n      creativeImageUrl\n      endDate\n      message\n      timerDuration\n      timer {\n        timeBombThreshold\n        daysLeftThreshold\n        dateDisplayThreshold\n        message\n        __typename\n      }\n      __typename\n    }\n    pricing(storeId: $storeId) {\n      message\n      original\n      mapAboveOriginalPrice\n      promotion {\n        dollarOff\n        type\n        description {\n          shortDesc\n          longDesc\n          __typename\n        }\n        percentageOff\n        promotionTag\n        savingsCenter\n        savingsCenterPromos\n        specialBuySavings\n        specialBuyDollarOff\n        specialBuyPercentageOff\n        dates {\n          end\n          start\n          __typename\n        }\n        experienceTag\n        subExperienceTag\n        __typename\n      }\n      value\n      alternatePriceDisplay\n      alternate {\n        bulk {\n          pricePerUnit\n          thresholdQuantity\n          value\n          __typename\n        }\n        unit {\n          caseUnitOfMeasure\n          unitsOriginalPrice\n          unitsPerCase\n          value\n          __typename\n        }\n        __typename\n      }\n      preferredPriceFlag\n      specialBuy\n      unitOfMeasure\n      clearance {\n        value\n        dollarOff\n        percentageOff\n        __typename\n      }\n      conditionalPromotions {\n        dates {\n          start\n          end\n          __typename\n        }\n        description {\n          shortDesc\n          longDesc\n          __typename\n        }\n        experienceTag\n        subExperienceTag\n        eligibilityCriteria {\n          itemGroup\n          minPurchaseAmount\n          minPurchaseQuantity\n          relatedSkusCount\n          omsSkus\n          __typename\n        }\n        reward {\n          tiers {\n            minPurchaseAmount\n            minPurchaseQuantity\n            rewardPercent\n            rewardAmountPerOrder\n            rewardAmountPerItem\n            rewardFixedPrice\n            maxAllowedRewardAmount\n            maxPurchaseQuantity\n            __typename\n          }\n          __typename\n        }\n        nvalues\n        brandRefinementId\n        __typename\n      }\n      __typename\n    }\n    reviews {\n      ratingsReviews {\n        totalReviews\n        averageRating\n        __typename\n      }\n      __typename\n    }\n    paintDetails(configId: $configId, storeId: $storeId) @skip(if: $skipPaintDetails) {\n      brandLogo\n      colorType\n      rgb {\n        red\n        green\n        blue\n        __typename\n      }\n      colorDisplayName\n      __typename\n    }\n    details {\n      collection {\n        url\n        collectionId\n        name\n        __typename\n      }\n      description\n      descriptiveAttributes {\n        name\n        value\n        bulleted\n        sequence\n        __typename\n      }\n      highlights\n      additionalResources {\n        infoAndGuides {\n          name\n          url\n          __typename\n        }\n        installationAndRentals {\n          contentType\n          name\n          url\n          __typename\n        }\n        diyProjects {\n          contentType\n          name\n          url\n          __typename\n        }\n        __typename\n      }\n      __typename\n    }\n    seo {\n      seoKeywords\n      seoDescription\n      __typename\n    }\n    specificationGroup @skip(if: $skipSpecificationGroup) {\n      specifications {\n        specName\n        specValue\n        __typename\n      }\n      specTitle\n      __typename\n    }\n    fulfillment(storeId: $storeId, zipCode: $zipCode, quantity: $quantity) {\n      fulfillmentOptions {\n        fulfillable\n        type\n        services {\n          type\n          locations {\n            isAnchor\n            locationId\n            inventory {\n              isOutOfStock\n              quantity\n              isInStock\n              isLimitedQuantity\n              isUnavailable\n              maxAllowedBopisQty\n              minAllowedBopisQty\n              __typename\n            }\n            curbsidePickupFlag\n            isBuyInStoreCheckNearBy\n            distance\n            storeName\n            state\n            type\n            storePhone\n            __typename\n          }\n          hasFreeShipping\n          freeDeliveryThreshold\n          deliveryTimeline\n          deliveryDates {\n            startDate\n            endDate\n            __typename\n          }\n          deliveryCharge\n          dynamicEta {\n            hours\n            minutes\n            __typename\n          }\n          optimalFulfillment\n          totalCharge\n          __typename\n        }\n        __typename\n      }\n      backordered\n      backorderedShipDate\n      bossExcludedShipStates\n      excludedShipStates\n      seasonStatusEligible\n      anchorStoreStatus\n      anchorStoreStatusType\n      sthExcludedShipState\n      bossExcludedShipState\n      onlineStoreStatus\n      onlineStoreStatusType\n      __typename\n    }\n    favoriteDetail @skip(if: $skipFavoriteCount) {\n      count\n      __typename\n    }\n    installServices(storeId: $storeId, zipCode: $zipCode) @skip(if: $skipInstallServices) {\n      scheduleAMeasure\n      gccCarpetDesignAndOrderEligible\n      __typename\n    }\n    keyProductFeatures @skip(if: $skipKPF) {\n      keyProductFeaturesItems {\n        features {\n          name\n          refinementId\n          refinementUrl\n          value\n          __typename\n        }\n        __typename\n      }\n      __typename\n    }\n    subscription @skip(if: $skipSubscribeAndSave) {\n      defaultfrequency\n      discountPercentage\n      subscriptionEnabled\n      __typename\n    }\n    sizeAndFitDetail {\n      attributeGroups {\n        attributes {\n          attributeName\n          dimensions\n          __typename\n        }\n        dimensionLabel\n        productType\n        __typename\n      }\n      __typename\n    }\n    dataSource\n    seoDescription\n    __typename\n  }\n}',
                }
                
                response = requests.post(
                    'https://nativeapp.homedepot.com/federation-gateway/graphql',
                    params=params,
                    headers=headers,
                    json=json_data,
                    proxies=proxies,
                    timeout=timeout,
                    verify=verify
                )

                if response.ok:success,result = True,response.json()
            
                elif response.status_code in [503,412,456,522,408,403] and retries > 0:
                    print(sku + ' at ' + store['store_name'] + ' failed to get details, retrying...')
                    success,result = self.get_product_details(store,delay,timeout,sku=sku,retries=retries-1)

                else:raise Exception(sku + ' at ' + store['store_name'] + ' failed to get details')

            else:
                headers.update({
                    'x-current-url': cat_url,
                    'x-experience-name': 'landing-page',
                    'x-thd-customer-token': '',
                })
                params = {
                    'opname': 'searchModel',
                }

                json_data = {
                    'operationName': 'searchModel',
                    'variables': {
                        'skipInstallServices': False,
                        'skipFavoriteCount': False,
                        'skipKPF': False,
                        'skipSpecificationGroup': False,
                        'skipSubscribeAndSave': False,
                        'storefilter': 'ALL',
                        'channel': 'MOBILE',
                        'skipDiscoveryZones': False,
                        'skipBuyitagain': True,
                        'additionalSearchParams': {
                            'sponsored': True,
                            'mcvisId': self.generate_sensor_data(type='mcvisId'),
                            'plp': True,
                            'deliveryZip': store['zipcode'],
                            'multiStoreIds': [],
                        },
                        'filter': {},
                        'navParam': cat_id,
                        # 'orderBy': {
                        #     'field': 'TOP_SELLERS',
                        #     'order': 'ASC',
                        # },
                        'pageSize': limit,
                        'startIndex': int(limit * offset),
                        'storeId': store['store_id'],
                        'zipCode': store['zipcode'],
                    },
                    'query': 'query searchModel($storeId: String, $zipCode: String, $skipInstallServices: Boolean = true, $startIndex: Int, $pageSize: Int, $orderBy: ProductSort, $filter: ProductFilter, $skipFavoriteCount: Boolean = false, $skipKPF: Boolean = false, $skipSpecificationGroup: Boolean = false, $skipSubscribeAndSave: Boolean = false, $keyword: String, $navParam: String, $storefilter: StoreFilter = ALL, $itemIds: [String], $channel: Channel = MOBILE, $additionalSearchParams: AdditionalParams, $loyaltyMembershipInput: LoyaltyMembershipInput, $dataSource: String, $skipDiscoveryZones: Boolean = true, $skipBuyitagain: Boolean = true) {\n  searchModel(\n    keyword: $keyword\n    navParam: $navParam\n    storefilter: $storefilter\n    storeId: $storeId\n    itemIds: $itemIds\n    channel: $channel\n    additionalSearchParams: $additionalSearchParams\n    loyaltyMembershipInput: $loyaltyMembershipInput\n  ) {\n    metadata {\n      hasPLPBanner\n      categoryID\n      analytics {\n        semanticTokens\n        dynamicLCA\n        __typename\n      }\n      canonicalUrl\n      searchRedirect\n      clearAllRefinementsURL\n      contentType\n      h1Tag\n      isStoreDisplay\n      productCount {\n        inStore\n        __typename\n      }\n      stores {\n        storeId\n        storeName\n        address {\n          postalCode\n          __typename\n        }\n        nearByStores {\n          storeId\n          storeName\n          distance\n          address {\n            postalCode\n            __typename\n          }\n          __typename\n        }\n        __typename\n      }\n      __typename\n    }\n    products(\n      startIndex: $startIndex\n      pageSize: $pageSize\n      orderBy: $orderBy\n      filter: $filter\n    ) {\n      identifiers {\n        storeSkuNumber\n        canonicalUrl\n        brandName\n        itemId\n        productLabel\n        productType\n        specialOrderSku\n        modelNumber\n        parentId\n        __typename\n      }\n      installServices(storeId: $storeId, zipCode: $zipCode) @skip(if: $skipInstallServices) {\n        scheduleAMeasure\n        gccCarpetDesignAndOrderEligible\n        __typename\n      }\n      itemId\n      dataSources\n      media {\n        images {\n          url\n          type\n          subType\n          sizes\n          __typename\n        }\n        __typename\n      }\n      pricing(storeId: $storeId) {\n        value\n        alternatePriceDisplay\n        alternate {\n          bulk {\n            pricePerUnit\n            thresholdQuantity\n            value\n            __typename\n          }\n          unit {\n            caseUnitOfMeasure\n            unitsOriginalPrice\n            unitsPerCase\n            value\n            __typename\n          }\n          __typename\n        }\n        original\n        mapAboveOriginalPrice\n        message\n        preferredPriceFlag\n        promotion {\n          type\n          description {\n            shortDesc\n            longDesc\n            __typename\n          }\n          dollarOff\n          percentageOff\n          promotionTag\n          savingsCenter\n          savingsCenterPromos\n          specialBuySavings\n          specialBuyDollarOff\n          specialBuyPercentageOff\n          dates {\n            start\n            end\n            __typename\n          }\n          __typename\n        }\n        specialBuy\n        unitOfMeasure\n        clearance {\n          value\n          dollarOff\n          percentageOff\n          __typename\n        }\n        __typename\n      }\n      reviews {\n        ratingsReviews {\n          averageRating\n          totalReviews\n          __typename\n        }\n        __typename\n      }\n      info {\n        swatches {\n          isSelected\n          itemId\n          label\n          swatchImgUrl\n          url\n          value\n          __typename\n        }\n        hidePrice\n        ecoRebate\n        quantityLimit\n        categoryHierarchy\n        sskMin\n        sskMax\n        unitOfMeasureCoverage\n        wasMaxPriceRange\n        wasMinPriceRange\n        productSubType {\n          name\n          link\n          __typename\n        }\n        customerSignal {\n          previouslyPurchased\n          __typename\n        }\n        isBuryProduct\n        isGenericProduct\n        returnable\n        isLiveGoodsProduct\n        isSponsored\n        sponsoredMetadata {\n          campaignId\n          placementId\n          slotId\n          sponsoredId\n          trackSource\n          __typename\n        }\n        globalCustomConfigurator {\n          customExperience\n          __typename\n        }\n        augmentedReality\n        sponsoredBeacon {\n          onClickBeacon\n          onViewBeacon\n          onClickBeacons\n          onViewBeacons\n          __typename\n        }\n        hasSubscription\n        samplesAvailable\n        totalNumberOfOptions\n        classNumber\n        productDepartment\n        __typename\n      }\n      details {\n        installation {\n          serviceType\n          __typename\n        }\n        collection {\n          name\n          url\n          collectionId\n          __typename\n        }\n        highlights\n        __typename\n      }\n      fulfillment(storeId: $storeId, zipCode: $zipCode) {\n        anchorStoreStatus\n        anchorStoreStatusType\n        backordered\n        backorderedShipDate\n        bossExcludedShipStates\n        excludedShipStates\n        seasonStatusEligible\n        fulfillmentOptions {\n          type\n          fulfillable\n          services {\n            deliveryTimeline\n            deliveryDates {\n              startDate\n              endDate\n              __typename\n            }\n            deliveryCharge\n            dynamicEta {\n              hours\n              minutes\n              __typename\n            }\n            hasFreeShipping\n            freeDeliveryThreshold\n            locations {\n              curbsidePickupFlag\n              isBuyInStoreCheckNearBy\n              distance\n              inventory {\n                isOutOfStock\n                isInStock\n                isLimitedQuantity\n                isUnavailable\n                quantity\n                maxAllowedBopisQty\n                minAllowedBopisQty\n                __typename\n              }\n              isAnchor\n              locationId\n              state\n              storeName\n              storePhone\n              type\n              __typename\n            }\n            type\n            totalCharge\n            __typename\n          }\n          __typename\n        }\n        onlineStoreStatus\n        onlineStoreStatusType\n        fulfillmentBundleMessage\n        __typename\n      }\n      taxonomy {\n        breadCrumbs {\n          label\n          __typename\n        }\n        __typename\n      }\n      availabilityType {\n        type\n        discontinued\n        buyable\n        status\n        __typename\n      }\n      badges(storeId: $storeId) {\n        name\n        label\n        __typename\n      }\n      dataSource\n      favoriteDetail @skip(if: $skipFavoriteCount) {\n        count\n        __typename\n      }\n      keyProductFeatures @skip(if: $skipKPF) {\n        keyProductFeaturesItems {\n          features {\n            name\n            refinementId\n            refinementUrl\n            value\n            __typename\n          }\n          __typename\n        }\n        __typename\n      }\n      specificationGroup @skip(if: $skipSpecificationGroup) {\n        specifications {\n          specName\n          specValue\n          __typename\n        }\n        specTitle\n        __typename\n      }\n      subscription @skip(if: $skipSubscribeAndSave) {\n        defaultfrequency\n        discountPercentage\n        subscriptionEnabled\n        __typename\n      }\n      bundleFlag\n      __typename\n    }\n    id\n    searchReport {\n      totalProducts\n      didYouMean\n      correctedKeyword\n      keyword\n      pageSize\n      searchUrl\n      sortBy\n      sortOrder\n      startIndex\n      __typename\n    }\n    relatedResults {\n      universalSearch {\n        title\n        __typename\n      }\n      relatedServices {\n        label\n        __typename\n      }\n      visualNavs {\n        label\n        imageId\n        webUrl\n        categoryId\n        imageURL\n        __typename\n      }\n      visualNavContainsEvents\n      relatedKeywords {\n        keyword\n        __typename\n      }\n      __typename\n    }\n    taxonomy {\n      brandLinkUrl\n      breadCrumbs {\n        browseUrl\n        creativeIconUrl\n        deselectUrl\n        dimensionId\n        dimensionName\n        label\n        refinementKey\n        url\n        __typename\n      }\n      __typename\n    }\n    templates\n    partialTemplates\n    dimensions {\n      label\n      refinements {\n        refinementKey\n        label\n        recordCount\n        selected\n        imgUrl\n        url\n        nestedRefinements {\n          label\n          url\n          recordCount\n          refinementKey\n          __typename\n        }\n        __typename\n      }\n      collapse\n      dimensionId\n      isVisualNav\n      isVisualDimension\n      isNumericFilter\n      isColorSwatch\n      nestedRefinementsLimit\n      visualNavSequence\n      __typename\n    }\n    orangeGraph {\n      universalSearchArray {\n        pods {\n          title\n          description\n          imageUrl\n          link\n          isProContent\n          recordType\n          __typename\n        }\n        info {\n          title\n          __typename\n        }\n        __typename\n      }\n      productTypes\n      __typename\n    }\n    discoveryZones @skip(if: $skipDiscoveryZones) {\n      products(dataSource: $dataSource) {\n        itemId\n        dataSources\n        badges(storeId: $storeId) {\n          name\n          __typename\n        }\n        info {\n          isSponsored\n          sponsoredMetadata {\n            campaignId\n            placementId\n            slotId\n            sponsoredId\n            trackSource\n            __typename\n          }\n          sponsoredBeacon {\n            onClickBeacon\n            onViewBeacon\n            onClickBeacons\n            onViewBeacons\n            __typename\n          }\n          productSubType {\n            name\n            __typename\n          }\n          augmentedReality\n          globalCustomConfigurator {\n            customExperience\n            __typename\n          }\n          swatches {\n            isSelected\n            itemId\n            label\n            swatchImgUrl\n            url\n            value\n            __typename\n          }\n          totalNumberOfOptions\n          hidePrice\n          ecoRebate\n          quantityLimit\n          categoryHierarchy\n          sskMin\n          sskMax\n          unitOfMeasureCoverage\n          wasMaxPriceRange\n          wasMinPriceRange\n          __typename\n        }\n        identifiers {\n          canonicalUrl\n          productType\n          productLabel\n          modelNumber\n          storeSkuNumber\n          itemId\n          brandName\n          parentId\n          __typename\n        }\n        media {\n          images {\n            url\n            type\n            subType\n            sizes\n            __typename\n          }\n          __typename\n        }\n        dataSource\n        details {\n          collection {\n            name\n            url\n            __typename\n          }\n          __typename\n        }\n        pricing(storeId: $storeId) {\n          alternatePriceDisplay\n          alternate {\n            bulk {\n              pricePerUnit\n              thresholdQuantity\n              value\n              __typename\n            }\n            unit {\n              caseUnitOfMeasure\n              unitsOriginalPrice\n              unitsPerCase\n              value\n              __typename\n            }\n            __typename\n          }\n          original\n          mapAboveOriginalPrice\n          message\n          preferredPriceFlag\n          promotion {\n            type\n            description {\n              shortDesc\n              longDesc\n              __typename\n            }\n            dollarOff\n            percentageOff\n            promotionTag\n            savingsCenter\n            savingsCenterPromos\n            specialBuySavings\n            specialBuyDollarOff\n            specialBuyPercentageOff\n            __typename\n          }\n          specialBuy\n          unitOfMeasure\n          value\n          __typename\n        }\n        __typename\n      }\n      metadata {\n        zone\n        zoneTitle\n        __typename\n      }\n      __typename\n    }\n    appliedDimensions {\n      label\n      refinements {\n        label\n        refinementKey\n        url\n        __typename\n      }\n      isNumericFilter\n      __typename\n    }\n    primaryFilters {\n      collapse\n      dimensionId\n      isVisualNav\n      isVisualDimension\n      isNumericFilter\n      isColorSwatch\n      label\n      nestedRefinementsLimit\n      refinements {\n        label\n        refinementKey\n        recordCount\n        selected\n        imgUrl\n        url\n        nestedRefinements {\n          label\n          url\n          recordCount\n          refinementKey\n          __typename\n        }\n        __typename\n      }\n      visualNavSequence\n      __typename\n    }\n    buyitagain(dataSource: $dataSource) @skip(if: $skipBuyitagain) {\n      itemId\n      dataSources\n      badges(storeId: $storeId) {\n        name\n        __typename\n      }\n      info {\n        isSponsored\n        sponsoredMetadata {\n          campaignId\n          placementId\n          slotId\n          sponsoredId\n          trackSource\n          __typename\n        }\n        sponsoredBeacon {\n          onClickBeacon\n          onViewBeacon\n          onClickBeacons\n          onViewBeacons\n          __typename\n        }\n        productSubType {\n          name\n          link\n          __typename\n        }\n        augmentedReality\n        globalCustomConfigurator {\n          customExperience\n          __typename\n        }\n        customerSignal {\n          previouslyPurchased\n          __typename\n        }\n        isBuryProduct\n        isGenericProduct\n        returnable\n        hidePrice\n        ecoRebate\n        quantityLimit\n        categoryHierarchy\n        sskMin\n        sskMax\n        unitOfMeasureCoverage\n        wasMaxPriceRange\n        wasMinPriceRange\n        __typename\n      }\n      identifiers {\n        canonicalUrl\n        productType\n        productLabel\n        modelNumber\n        storeSkuNumber\n        itemId\n        brandName\n        specialOrderSku\n        __typename\n      }\n      media {\n        images {\n          url\n          type\n          subType\n          sizes\n          __typename\n        }\n        __typename\n      }\n      details {\n        installation {\n          serviceType\n          __typename\n        }\n        collection {\n          name\n          url\n          __typename\n        }\n        __typename\n      }\n      fulfillment(storeId: $storeId, zipCode: $zipCode) {\n        anchorStoreStatus\n        anchorStoreStatusType\n        backordered\n        backorderedShipDate\n        bossExcludedShipStates\n        excludedShipStates\n        seasonStatusEligible\n        fulfillmentOptions {\n          type\n          fulfillable\n          services {\n            deliveryTimeline\n            deliveryDates {\n              startDate\n              endDate\n              __typename\n            }\n            deliveryCharge\n            dynamicEta {\n              hours\n              minutes\n              __typename\n            }\n            hasFreeShipping\n            freeDeliveryThreshold\n            locations {\n              curbsidePickupFlag\n              isBuyInStoreCheckNearBy\n              distance\n              inventory {\n                isOutOfStock\n                isInStock\n                isLimitedQuantity\n                isUnavailable\n                quantity\n                maxAllowedBopisQty\n                minAllowedBopisQty\n                __typename\n              }\n              isAnchor\n              locationId\n              state\n              storeName\n              storePhone\n              type\n              __typename\n            }\n            type\n            totalCharge\n            __typename\n          }\n          __typename\n        }\n        onlineStoreStatus\n        onlineStoreStatusType\n        __typename\n      }\n      installServices(storeId: $storeId, zipCode: $zipCode) @skip(if: $skipInstallServices) {\n        scheduleAMeasure\n        gccCarpetDesignAndOrderEligible\n        __typename\n      }\n      taxonomy {\n        breadCrumbs {\n          label\n          __typename\n        }\n        __typename\n      }\n      pricing(storeId: $storeId) {\n        alternatePriceDisplay\n        alternate {\n          bulk {\n            pricePerUnit\n            thresholdQuantity\n            value\n            __typename\n          }\n          unit {\n            caseUnitOfMeasure\n            unitsOriginalPrice\n            unitsPerCase\n            value\n            __typename\n          }\n          __typename\n        }\n        original\n        mapAboveOriginalPrice\n        message\n        preferredPriceFlag\n        promotion {\n          type\n          description {\n            shortDesc\n            longDesc\n            __typename\n          }\n          dollarOff\n          percentageOff\n          promotionTag\n          savingsCenter\n          savingsCenterPromos\n          specialBuySavings\n          specialBuyDollarOff\n          specialBuyPercentageOff\n          __typename\n        }\n        specialBuy\n        unitOfMeasure\n        value\n        __typename\n      }\n      dataSource\n      __typename\n    }\n    __typename\n  }\n}',
                }

                response = requests.post(
                    'https://nativeapp.homedepot.com/federation-gateway/graphql',
                    params=params,
                    headers=headers,
                    json=json_data,
                    proxies=proxies,
                    timeout=timeout,
                    verify=verify
                )
                
                if response.ok:
                    if response.json().get('errors'):
                        raise Exception(response.json().get('errors',[{}])[0].get('message','Unknown error'))
                    success,result = True,response.json()
            
                elif response.status_code in [503,412,456,522,408,403]:
                    print('Whole store search' + ' at ' + store['store_name'] + ' failed to get details, retrying...')
                    success,result = self.get_product_details(
                        store,delay,timeout,limit=limit,
                        offset=offset,cat_id=cat_id,multiple=multiple,retries=retries-1)

                else:raise Exception(response.text)
        
        except ProxyError as pe:
            print(f'Error getting data: {str(pe)} at {store["store_name"]}  retrying...')
            if retries > 0:
                success,result = self.get_product_details(
                            store,delay,timeout,sku=sku,limit=limit,
                            offset=offset,cat_id=cat_id,multiple=multiple,retries=retries-1)

        except ConnectionError as ce:
            print(f'Error getting data: {str(ce)} at {store["store_name"]}  retrying...')
            if retries > 0:
                success,result = self.get_product_details(
                            store,delay,timeout,sku=sku,limit=limit,
                            offset=offset,cat_id=cat_id,multiple=multiple,retries=retries-1)

            
        except Exception as error:
            success,result = False,f"error getting product: {error}"
        
        return success,result
    
    def load_stores(self):
        # Load store details from store_list.csv
        stores = []
        stores_file = os.path.join(self.root_dir, 'store_list.csv')
        with open(stores_file, 'r', encoding='utf-8') as csvf:
            reader = csv.DictReader(csvf)
            for row in reader:
                store = {
                    'store_id': row['Store #'],
                    'store_name': row['Store Name'],
                    'address': row['Store Address'],
                    'city': row['Store City'],
                    'zipcode': row['Store ZIp'],
                    'state': row['State'],
                    'latitude': str(row.get('LAT1','')).strip(),
                    'longitude': str(row.get('LONG1','')).strip(),
                    'District': row.get('District#','')
                }
                # Include any additional fields dynamically
                for key, value in row.items():
                    if key not in ['Store #', 'Store Name', 'Store Address', 'Store City', 'Store ZIp', 'State', 'LAT1', 'LONG1', 'ts_datetime', 'District#']:
                        store[key] = value
                stores.append(store)
        return stores

    def scan_items(self, store, cat, limit, offset, delay, timeout):
        try:
            success, items = self.get_product_details(
                store, delay, timeout, limit=limit, offset=offset, cat_id=cat['categoryId'], cat_url=cat['link'], multiple=True
            )
            
            if not success:
                raise Exception(str(items))
                
            results = items['data']['searchModel']['products']
            
            # This will hold all processed rows to be returned
            all_item_rows = [] 

            with concurrent.futures.ThreadPoolExecutor(max_workers=limit) as executor:
                print(f'\nGetting prices from {len(results)} items on {store["store_name"]}...\n')
                
                # Map futures back to their original item data if needed later
                future_to_item_id = {}
                for result_item in results:
                    args = (store, result_item['itemId'], delay, timeout)
                    kwargs = {'data': result_item, 'multiple': True}
                    
                    future = executor.submit(self.format_data, *args, **kwargs)
                    future_to_item_id[future] = result_item['itemId']

                # Iterate over completed futures to collect results efficiently
                for future in concurrent.futures.as_completed(future_to_item_id):
                    item_id = future_to_item_id[future]
                    try:
                        success, item_result = future.result()
                        
                        if success:
                            # Assuming check_prices returns a list/tuple of values structured for CSV
                            # e.g., result = (name, brand, url, ...)
                            all_item_rows.append(item_result.values())
                            store_id = store['store_id'] # Use the store from the outer scope
                            print(f'{item_id} added to csv on store {store_id}')
                        else: 
                            print(f"Error for item {item_id}: {item_result.get('message', 'Unknown error')}")
                    
                    except Exception as e:
                        print(f"Exception processing item {item_id}: {e}")

            # Return the collected data (list of lists/tuples)
            return True, {"store": store['store_id'], "data": all_item_rows}
            
        except Exception as error:
            return False, {"store": store['store_id'], "message": str(error)}
        

    def scan_wholestore(self,store, csv_file, total_items=1800, limit=50, offset=0, delay=3, timeout=60):
        try:
            scanned = 0

            success,cats = self.load_categories()
            if not success:return False,cats

            print(f'Scanning the entire {self.name} for price drops\n')

            
            
            store_id = store['store_id']
            print(f'Starting scan for store: {store["store_name"]} (ID: {store["store_id"]})\n')
            
            # Calculation for 'base' needs review based on your scraping logic
            # For now, keeping your original calculation logic:
            # base = total_items // limit
            # if total_items % limit == 0:base += 1

            while scanned < total_items:
                if scanned >= total_items:
                    print(f'Reached total item limit of {total_items}, for store {store_id}.')
                    break

                success, result_data = self.scan_items( store, random.choice(cats), limit, offset, delay, timeout)

                if success:
                    data = result_data['data']
                    print(f'Successfully scanned store {store_id}, total items: {len(data)}, scanned so far: {scanned + len(data)} at offset {offset}')
                    

                    with open(csv_file, 'a', encoding='utf-8', newline='') as f:
                        writer = csv.writer(f)
                        writer.writerows(data)
                    
                    scanned += len(data)

                else:
                    message = result_data['message']
                    print(f'Failed to scan store {store_id}: {message}')

                # Update offset for the next iteration of the outer loop
                if offset * limit >= 720:offset = 0
                else:offset += 1

            return True, f'Completed scanning store {store_id}, total items scanned: {scanned}'
        except Exception as error:
            print(f'Error scanning whole store: {error}')
            return False, str(error)



if __name__ == '__main__':
    # homedepot = HomeDepot(proxies={
    #     'http': 'http://127.0.0.1:8080',
    #     'https': 'http://127.0.0.1:8080',
    # })

    homedepot = HomeDepot()

    stores = homedepot.load_stores()
    headers = ['name', 'brand', 'url', 'mainImageurl', 'SKU', 'Reviews', 'Rating', 'Model', 'retailer', 'storesku', 'omsid','storeName','storeID','storeLocation','inventory']
    csv_file = f'product-{datetime.now().strftime("%Y-%m-%d")}.csv'
    results_folder = os.path.join(homedepot.root_dir, 'results')

    if not os.path.exists(results_folder):
        os.makedirs(results_folder)

    if not os.path.isfile(f'{results_folder}/{csv_file}'):
        with open(f'{results_folder}/{csv_file}', 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(headers)


    for store in stores:
        success, result = homedepot.scan_wholestore(
            store,
            f'{results_folder}/{csv_file}',
            total_items=1800,
            limit=50,
            offset=0,
            delay=2,
            timeout=60
        )

        Utils.deduplicate_csv(f'{results_folder}/{csv_file}')
        print(result)   
